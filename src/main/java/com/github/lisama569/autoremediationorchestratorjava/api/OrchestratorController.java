package com.github.lisama569.autoremediationorchestratorjava.api;

import com.github.lisama569.autoremediationorchestratorjava.model.FaultInjectionPlan;
import com.github.lisama569.autoremediationorchestratorjava.model.FaultType;
import com.github.lisama569.autoremediationorchestratorjava.model.SubmissionResult;
import com.github.lisama569.autoremediationorchestratorjava.model.Target;
import com.github.lisama569.autoremediationorchestratorjava.service.PlanOrchestrator;
import com.github.lisama569.autoremediationorchestratorjava.service.SimulatedRemediationAction;
import com.github.lisama569.autoremediationorchestratorjava.service.TargetRegistry;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** 故障注入与修复编排的 REST 入口。 */
@RestController
@RequestMapping("/api")
public class OrchestratorController {

    private final PlanOrchestrator orchestrator;
    private final TargetRegistry registry;
    private final SimulatedRemediationAction action;

    public OrchestratorController(PlanOrchestrator orchestrator, TargetRegistry registry,
                                  SimulatedRemediationAction action) {
        this.orchestrator = orchestrator;
        this.registry = registry;
        this.action = action;
    }

    public record RegisterTargetRequest(String targetId, boolean inMaintenanceWindow) {}
    public record SubmitPlanRequest(String targetId, FaultType faultType, String idempotencyKey) {}

    @PostMapping("/targets")
    public Target registerTarget(@RequestBody RegisterTargetRequest request) {
        Target target = Target.healthy(request.targetId(), request.inMaintenanceWindow());
        registry.register(target);
        return target;
    }

    @GetMapping("/targets/{targetId}")
    public ResponseEntity<Target> getTarget(@PathVariable String targetId) {
        return registry.get(targetId).map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping("/targets/{targetId}/maintenance")
    public Target setMaintenanceWindow(@PathVariable String targetId, @RequestParam boolean open) {
        return registry.update(targetId, t -> t.withMaintenanceWindow(open));
    }

    @PostMapping("/targets/{targetId}/recover")
    public Target recoverTarget(@PathVariable String targetId) {
        return orchestrator.recoverTarget(targetId);
    }

    /** 测试/演示钩子：设置该目标修复动作的模拟行为。 */
    @PostMapping("/targets/{targetId}/behavior")
    public void setBehavior(@PathVariable String targetId,
                            @RequestParam SimulatedRemediationAction.Behavior behavior) {
        action.setBehavior(targetId, behavior);
    }

    @PostMapping("/plans")
    public ResponseEntity<SubmissionResult> submitPlan(@RequestBody SubmitPlanRequest request) {
        SubmissionResult result = orchestrator.submit(
                request.targetId(), request.faultType(), request.idempotencyKey());
        return result.accepted()
                ? ResponseEntity.accepted().body(result)
                : ResponseEntity.status(HttpStatus.CONFLICT).body(result);
    }

    @GetMapping("/plans/{planId}")
    public ResponseEntity<FaultInjectionPlan> getPlan(@PathVariable String planId) {
        return orchestrator.getPlan(planId).map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
