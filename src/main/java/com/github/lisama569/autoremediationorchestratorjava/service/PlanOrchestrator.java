package com.github.lisama569.autoremediationorchestratorjava.service;

import com.github.lisama569.autoremediationorchestratorjava.model.FaultInjectionPlan;
import com.github.lisama569.autoremediationorchestratorjava.model.FaultType;
import com.github.lisama569.autoremediationorchestratorjava.model.PlanStatus;
import com.github.lisama569.autoremediationorchestratorjava.model.RemediationPolicy;
import com.github.lisama569.autoremediationorchestratorjava.model.RejectionReason;
import com.github.lisama569.autoremediationorchestratorjava.model.SubmissionResult;
import com.github.lisama569.autoremediationorchestratorjava.model.Target;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 故障注入计划的编排入口。
 *
 * 校验顺序固定（保证拒绝原因稳定可区分）：
 * 1. 重复下发（幂等键已存在）→ DUPLICATE_REQUEST
 * 2. 目标已隔离 → TARGET_QUARANTINED
 * 3. 未处于维护窗口 → OUTSIDE_MAINTENANCE_WINDOW
 * 4. 同一目标已有进行中的计划 → PLAN_CONFLICT
 *
 * 通过后异步执行：注入故障 → 自动修复 → 推进到终态（SUCCEEDED / ROLLED_BACK / FAILED）。
 */
@Service
public class PlanOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(PlanOrchestrator.class);

    private final TargetRegistry registry;
    private final RemediationEngine engine;
    private final RemediationPolicy policy;
    private final ExecutorService executor = Executors.newFixedThreadPool(8);

    private final Map<String, FaultInjectionPlan> plans = new ConcurrentHashMap<>();
    /** 幂等键 → 已接受计划的 planId */
    private final Map<String, String> idempotencyIndex = new ConcurrentHashMap<>();
    /** 目标 → 进行中计划的 planId（同一目标同一时刻只允许一个） */
    private final Map<String, String> activePlans = new ConcurrentHashMap<>();

    @org.springframework.beans.factory.annotation.Autowired
    public PlanOrchestrator(TargetRegistry registry, RemediationEngine engine) {
        this(registry, engine, RemediationPolicy.defaultPolicy());
    }

    public PlanOrchestrator(TargetRegistry registry, RemediationEngine engine, RemediationPolicy policy) {
        this.registry = registry;
        this.engine = engine;
        this.policy = policy;
    }

    public SubmissionResult submit(String targetId, FaultType faultType, String idempotencyKey) {
        Target target = registry.get(targetId)
                .orElseThrow(() -> new IllegalArgumentException("target not registered: " + targetId));

        // 1. 重复下发：返回已存在的计划，不造成重复执行
        String existingPlanId = idempotencyIndex.get(idempotencyKey);
        if (existingPlanId != null) {
            FaultInjectionPlan existing = plans.get(existingPlanId);
            log.info("[submit] target={} key={} 判定=DUPLICATE_REQUEST, 已存在计划plan={} status={}",
                    targetId, idempotencyKey, existingPlanId, existing.status());
            return SubmissionResult.rejected(existing, RejectionReason.DUPLICATE_REQUEST);
        }

        FaultInjectionPlan plan = FaultInjectionPlan.newPlan(targetId, faultType, idempotencyKey);

        // 2. 目标已隔离
        if (target.quarantined()) {
            return reject(plan, RejectionReason.TARGET_QUARANTINED,
                    "目标已隔离, consecutiveFailures=" + target.consecutiveFailures());
        }
        // 3. 未处于维护窗口
        if (!target.inMaintenanceWindow()) {
            return reject(plan, RejectionReason.OUTSIDE_MAINTENANCE_WINDOW, "目标未处于维护窗口");
        }
        // 4. 同一目标计划冲突（原子占位）
        if (activePlans.putIfAbsent(targetId, plan.planId()) != null) {
            return reject(plan, RejectionReason.PLAN_CONFLICT,
                    "目标已有进行中的计划plan=" + activePlans.get(targetId));
        }
        // 幂等键原子登记；并发下同键竞争失败者释放占位并按重复处理
        if (idempotencyIndex.putIfAbsent(idempotencyKey, plan.planId()) != null) {
            activePlans.remove(targetId, plan.planId());
            FaultInjectionPlan existing = plans.get(idempotencyIndex.get(idempotencyKey));
            log.info("[submit] plan={} target={} key={} 判定=DUPLICATE_REQUEST(并发竞争)",
                    plan.planId(), targetId, idempotencyKey);
            return SubmissionResult.rejected(existing, RejectionReason.DUPLICATE_REQUEST);
        }

        plans.put(plan.planId(), plan);
        log.info("[submit] plan={} target={} fault={} key={} 判定=ACCEPTED",
                plan.planId(), targetId, faultType, idempotencyKey);
        executor.submit(() -> execute(plan.planId()));
        return SubmissionResult.accepted(plan);
    }

    private SubmissionResult reject(FaultInjectionPlan plan, RejectionReason reason, String detail) {
        FaultInjectionPlan rejected = plan.rejected(reason);
        plans.put(rejected.planId(), rejected);
        log.info("[submit] plan={} target={} 判定={} 依据: {}", plan.planId(), plan.targetId(), reason, detail);
        return SubmissionResult.rejected(rejected, reason);
    }

    /** 执行链路：注入故障 → 修复 → 推进终态。任何异常都记录并落为 FAILED，不静默吞掉。 */
    private void execute(String planId) {
        FaultInjectionPlan plan = plans.get(planId);
        String targetId = plan.targetId();
        try {
            transition(planId, PlanStatus.RUNNING);
            RemediationEngine.RemediationOutcome outcome = engine.remediate(plan, policy);
            PlanStatus terminal = outcome == RemediationEngine.RemediationOutcome.SUCCESS
                    ? PlanStatus.SUCCEEDED : PlanStatus.ROLLED_BACK;
            transition(planId, terminal);
            Target after = registry.get(targetId).orElseThrow();
            log.info("[execute] plan={} target={} 终态={} 判定依据: 修复结果={}, 目标health={} quarantined={} consecutiveFailures={}",
                    planId, targetId, terminal, outcome, after.health(), after.quarantined(), after.consecutiveFailures());
        } catch (Exception e) {
            log.error("[execute] plan={} target={} 执行链路异常, 终态=FAILED: {}", planId, targetId, e.toString());
            transition(planId, PlanStatus.FAILED);
        } finally {
            activePlans.remove(targetId, planId);
        }
    }

    private void transition(String planId, PlanStatus status) {
        plans.computeIfPresent(planId, (id, p) -> p.withStatus(status));
    }

    public Optional<FaultInjectionPlan> getPlan(String planId) {
        return Optional.ofNullable(plans.get(planId));
    }

    /** 恢复被隔离的目标：解除隔离并清零连续失败计数，使其重新参与后续调度。 */
    public Target recoverTarget(String targetId) {
        Target recovered = registry.update(targetId, Target::release);
        log.info("[recover] target={} 已解除隔离, quarantined={} consecutiveFailures={}",
                targetId, recovered.quarantined(), recovered.consecutiveFailures());
        return recovered;
    }
}
