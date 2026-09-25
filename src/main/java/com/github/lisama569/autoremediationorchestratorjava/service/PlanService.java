package com.github.lisama569.autoremediationorchestratorjava.service;

import com.github.lisama569.autoremediationorchestratorjava.domain.AuditEvent;
import com.github.lisama569.autoremediationorchestratorjava.domain.FaultInjectionPlan;
import com.github.lisama569.autoremediationorchestratorjava.domain.PolicyConfig;
import com.github.lisama569.autoremediationorchestratorjava.domain.RejectReason;
import com.github.lisama569.autoremediationorchestratorjava.domain.SubmitResult;
import com.github.lisama569.autoremediationorchestratorjava.domain.Target;
import com.github.lisama569.autoremediationorchestratorjava.domain.TargetState;
import com.github.lisama569.autoremediationorchestratorjava.engine.ActionExecutor;
import com.github.lisama569.autoremediationorchestratorjava.engine.AuditLog;
import com.github.lisama569.autoremediationorchestratorjava.engine.Clock;
import com.github.lisama569.autoremediationorchestratorjava.engine.FaultAction;
import com.github.lisama569.autoremediationorchestratorjava.engine.TargetRegistry;
import com.github.lisama569.autoremediationorchestratorjava.engine.TimeoutRuntimeException;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 故障注入计划下发与执行。
 *
 * <p>判定顺序（同一目标锁内，可复现）：</p>
 * <ol>
 *   <li>幂等键去重（{@link RejectReason#DUPLICATE_REQUEST}）</li>
 *   <li>维护窗口（{@link RejectReason#OUTSIDE_MAINTENANCE_WINDOW}）</li>
 *   <li>隔离状态（{@link RejectReason#TARGET_QUARANTINED}）</li>
 *   <li>进行中计划（{@link RejectReason#PLAN_ALREADY_IN_PROGRESS}）</li>
 * </ol>
 *
 * <p>并发模型（关键）：目标锁只保护"检查 + 占位"与"终态推进 + 回滚"两个临界区；
 * 注入动作本身在锁外执行。占位 {@code activePlanId} 在动作期间非空，因此并发
 * 竞争请求进入临界区后会被<b>立即</b>拒绝为 {@code PLAN_ALREADY_IN_PROGRESS}，
 * 而不会排队等待，也不会有两个计划同时执行。动作超时/异常时在锁内用执行前快照
 * 精确回滚，回滚失败抛 {@code RollbackException}，不被静默吞掉。</p>
 */
public final class PlanService {

    private final TargetRegistry registry;
    private final AuditLog auditLog;
    private final Clock clock;
    private final Map<String, FaultAction> actions;
    private final ActionExecutor executor;
    private final PolicyConfig policy;

    /** 已接受计划的幂等键（跨目标全局去重）。 */
    private final Set<Long> acceptedKeys = ConcurrentHashMap.newKeySet();
    /** 进行中计划集合（实际互斥由目标锁 + activePlanId 保证）。 */
    private final Set<String> inProgressPlans = ConcurrentHashMap.newKeySet();

    public PlanService(TargetRegistry registry,
                       AuditLog auditLog,
                       Clock clock,
                       PolicyConfig policy,
                       Map<String, FaultAction> actions,
                       ActionExecutor executor) {
        this.registry = registry;
        this.auditLog = auditLog;
        this.clock = clock;
        this.policy = policy;
        this.actions = Map.copyOf(actions);
        this.executor = executor;
    }

    public SubmitResult submit(FaultInjectionPlan plan) {
        long now = clock.nowMs();
        Target target = registry.find(plan.targetId()).orElse(null);
        if (target == null) {
            throw new IllegalArgumentException("unknown target: " + plan.targetId());
        }

        Target.Snapshot snapshot;
        // ---- 临界区 1：检查 + 占位 ----
        synchronized (registry.lockFor(plan.targetId())) {
            if (acceptedKeys.contains(plan.idempotencyKey())) {
                return reject(plan, now, RejectReason.DUPLICATE_REQUEST,
                        "idempotency key already accepted: " + plan.idempotencyKey(),
                        "idempotencyKey=" + plan.idempotencyKey());
            }
            if (!plan.window().contains(now)) {
                return reject(plan, now, RejectReason.OUTSIDE_MAINTENANCE_WINDOW,
                        "now " + now + " outside window ["
                                + plan.window().startEpochMs() + ","
                                + plan.window().endEpochMs() + ")",
                        "now=" + now + " window=[" + plan.window().startEpochMs()
                                + "," + plan.window().endEpochMs() + ")");
            }
            if (target.state() == TargetState.QUARANTINED) {
                return reject(plan, now, RejectReason.TARGET_QUARANTINED,
                        "target is quarantined: " + plan.targetId(),
                        "target quarantined");
            }
            if (target.activePlanId() != null) {
                return reject(plan, now, RejectReason.PLAN_ALREADY_IN_PROGRESS,
                        "plan already in progress: " + target.activePlanId(),
                        "activePlan=" + target.activePlanId());
            }

            acceptedKeys.add(plan.idempotencyKey());
            target.beginPlan(plan.planId());
            inProgressPlans.add(plan.planId());
            plan.markRunning();
            snapshot = target.snapshot();
            auditLog.record(new AuditEvent(now, plan.targetId(), plan.planId(),
                    "SUBMIT", "ACCEPTED", "faultType=" + plan.faultType()));
        }

        // ---- 锁外执行注入动作（目标被 activePlanId 占位） ----
        boolean actionOk;
        RuntimeException actionError = null;
        FaultAction action = actions.get(plan.faultType().name());
        if (action == null) {
            actionOk = false;
            actionError = new IllegalStateException(
                    "no fault action bound for " + plan.faultType());
        } else {
            try {
                executor.runWithTimeout("inject:" + plan.planId(),
                        policy.injectionTimeoutMs(),
                        () -> action.apply(target));
                actionOk = true;
            } catch (RuntimeException e) {
                actionOk = false;
                actionError = e;
            }
        }

        // ---- 临界区 2：终态推进 / 回滚 ----
        synchronized (registry.lockFor(plan.targetId())) {
            String injectOutcome;
            String detail;
            if (actionOk) {
                target.markUnhealthy(plan.faultType().name());
                plan.markCompleted();
                injectOutcome = "COMPLETED";
                detail = "faultType=" + plan.faultType() + " targetState=" + target.state();
            } else {
                // 精确回滚到执行前快照；回滚失败抛 RollbackException，不被吞掉
                try {
                    target.restore(snapshot);
                } catch (RuntimeException rb) {
                    throw new com.github.lisama569.autoremediationorchestratorjava.engine.RollbackException(
                            "rollback failed for plan " + plan.planId()
                                    + " target " + plan.targetId(), rb);
                }
                plan.markFailed();
                injectOutcome = actionError instanceof TimeoutRuntimeException
                        ? "TIMED_OUT" : "FAILED";
                detail = "rolledBack=true detail=" + actionError.getMessage()
                        + " restoredState=" + target.state()
                        + " errorClass=" + actionError.getClass().getSimpleName();
            }
            inProgressPlans.remove(plan.planId());
            target.endPlan();
            auditLog.record(new AuditEvent(clock.nowMs(), target.id(), plan.planId(),
                    "INJECT", injectOutcome, detail));

            SubmitResult result = SubmitResult.accepted(plan);
            auditLog.record(new AuditEvent(clock.nowMs(), plan.targetId(), plan.planId(),
                    "SUBMIT", "ACCEPTED",
                    "finalPlanStatus=" + plan.status()
                            + " targetState=" + target.state()));
            return result;
        }
    }

    private SubmitResult reject(FaultInjectionPlan plan, long now,
                                RejectReason reason, String message, String basis) {
        auditLog.record(new AuditEvent(now, plan.targetId(), plan.planId(),
                "SUBMIT", "REJECTED", reason.name() + ": " + basis));
        return SubmitResult.rejected(reason, message);
    }

    public boolean isInProgress(String planId) {
        return inProgressPlans.contains(planId);
    }
}
