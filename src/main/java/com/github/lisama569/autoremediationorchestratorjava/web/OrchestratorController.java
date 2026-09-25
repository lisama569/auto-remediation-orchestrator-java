package com.github.lisama569.autoremediationorchestratorjava.web;

import com.github.lisama569.autoremediationorchestratorjava.domain.AuditEvent;
import com.github.lisama569.autoremediationorchestratorjava.domain.FaultType;
import com.github.lisama569.autoremediationorchestratorjava.domain.MaintenanceWindow;
import com.github.lisama569.autoremediationorchestratorjava.domain.RepairResult;
import com.github.lisama569.autoremediationorchestratorjava.domain.SubmitResult;
import com.github.lisama569.autoremediationorchestratorjava.domain.Target;
import com.github.lisama569.autoremediationorchestratorjava.service.Orchestrator;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * HTTP 门面：仅做参数绑定与状态码映射，全部判定逻辑位于 service 层。
 * 拒绝返回 409 且 body 中带可区分的 rejectReason；成功返回 200。
 */
@RestController
@RequestMapping("/api")
public class OrchestratorController {

    /** 计划下发请求体。 */
    public record PlanRequest(String planId,
                              String targetId,
                              FaultType faultType,
                              Long idempotencyKey,
                              Long windowStartMs,
                              Long windowEndMs) {
    }

    public record TargetView(String targetId,
                             String state,
                             int consecutiveFailures,
                             String activePlanId,
                             boolean repairRunning,
                             long nextEligibleAtMs,
                             String activeFault) {
        static TargetView of(Target t) {
            return new TargetView(t.id(), t.state().name(), t.consecutiveFailures(),
                    t.activePlanId(), t.repairRunning(), t.nextEligibleAtMs(), t.activeFault());
        }
    }

    private final Orchestrator orchestrator;

    public OrchestratorController(Orchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @PostMapping("/targets/{targetId}")
    public TargetView register(@PathVariable String targetId) {
        return TargetView.of(orchestrator.registerTarget(targetId));
    }

    @GetMapping("/targets/{targetId}")
    public TargetView status(@PathVariable String targetId) {
        return TargetView.of(orchestrator.getTarget(targetId));
    }

    @PostMapping("/plans")
    public ResponseEntity<SubmitResult> submit(@RequestBody PlanRequest req) {
        SubmitResult result = orchestrator.submitPlan(req.planId(), req.targetId(),
                req.faultType(), req.idempotencyKey(),
                new MaintenanceWindow(req.windowStartMs(), req.windowEndMs()));
        return result.accepted()
                ? ResponseEntity.ok(result)
                : ResponseEntity.status(HttpStatus.CONFLICT).body(result);
    }

    @PostMapping("/targets/{targetId}/repair")
    public RepairResult repair(@PathVariable String targetId) {
        return orchestrator.repair(targetId);
    }

    @PostMapping("/targets/{targetId}/recover")
    public Map<String, Object> recover(@PathVariable String targetId) {
        return Map.of("targetId", targetId, "recovered", orchestrator.recover(targetId));
    }

    @GetMapping("/audit")
    public List<AuditEvent> audit() {
        return orchestrator.auditLog().events();
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
}
