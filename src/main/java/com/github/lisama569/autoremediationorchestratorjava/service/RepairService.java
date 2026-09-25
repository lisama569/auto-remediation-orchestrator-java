package com.github.lisama569.autoremediationorchestratorjava.service;

import com.github.lisama569.autoremediationorchestratorjava.domain.AuditEvent;
import com.github.lisama569.autoremediationorchestratorjava.domain.PolicyConfig;
import com.github.lisama569.autoremediationorchestratorjava.domain.RepairOutcome;
import com.github.lisama569.autoremediationorchestratorjava.domain.RepairResult;
import com.github.lisama569.autoremediationorchestratorjava.domain.Target;
import com.github.lisama569.autoremediationorchestratorjava.domain.TargetState;
import com.github.lisama569.autoremediationorchestratorjava.engine.ActionExecutor;
import com.github.lisama569.autoremediationorchestratorjava.engine.AuditLog;
import com.github.lisama569.autoremediationorchestratorjava.engine.Clock;
import com.github.lisama569.autoremediationorchestratorjava.engine.HealthProbe;
import com.github.lisama569.autoremediationorchestratorjava.engine.RepairAction;
import com.github.lisama569.autoremediationorchestratorjava.engine.TargetRegistry;
import com.github.lisama569.autoremediationorchestratorjava.engine.TimeoutRuntimeException;

/**
 * 自动修复触发、退避、隔离与回滚。
 *
 * <p>并发模型与 {@code PlanService} 对称：目标锁保护"前置检查 + 占位
 * (repairRunning)"与"终态推进 + 回滚 + 计数"两个临界区；修复动作与健康
 * 校验在锁外执行。前置跳过（隔离中/健康/已有修复/有注入计划/退避窗口内）
 * 一律不计数；真正执行过动作的尝试才计数：</p>
 * <ul>
 *   <li>超时 / 动作异常 / 健康校验失败：终态临界区内先回滚到执行前快照，
 *       再 +1，计数不会被回滚撤销，也不会因并发重复或丢失；</li>
 *   <li>连续失败达到阈值：目标进入 QUARANTINED 并停止继续重试；</li>
 *   <li>成功：目标恢复 HEALTHY、计数归零、退避清除。</li>
 * </ul>
 */
public final class RepairService {

    private final TargetRegistry registry;
    private final AuditLog auditLog;
    private final Clock clock;
    private final PolicyConfig policy;
    private final RepairAction repairAction;
    private final HealthProbe healthProbe;
    private final ActionExecutor executor;

    public RepairService(TargetRegistry registry,
                         AuditLog auditLog,
                         Clock clock,
                         PolicyConfig policy,
                         RepairAction repairAction,
                         HealthProbe healthProbe,
                         ActionExecutor executor) {
        this.registry = registry;
        this.auditLog = auditLog;
        this.clock = clock;
        this.policy = policy;
        this.repairAction = repairAction;
        this.healthProbe = healthProbe;
        this.executor = executor;
    }

    public RepairResult triggerRepair(String targetId) {
        long now = clock.nowMs();
        Target target = registry.find(targetId).orElse(null);
        if (target == null) {
            auditLog.record(new AuditEvent(now, targetId, null,
                    "REPAIR", "UNKNOWN_TARGET", "target not registered"));
            return new RepairResult(targetId, RepairOutcome.UNKNOWN_TARGET,
                    0, null, 0L, "target not registered");
        }

        Target.Snapshot snapshot;
        // ---- 临界区 1：前置检查 + 占位 ----
        synchronized (registry.lockFor(targetId)) {
            TargetState state = target.state();
            if (state == TargetState.QUARANTINED) {
                return skip(target, RepairOutcome.ALREADY_QUARANTINED,
                        "target quarantined; retries stopped");
            }
            if (state == TargetState.HEALTHY) {
                return skip(target, RepairOutcome.ALREADY_HEALTHY,
                        "target already healthy");
            }
            if (target.repairRunning()) {
                return skip(target, RepairOutcome.ALREADY_RUNNING,
                        "a repair attempt is already running");
            }
            if (target.activePlanId() != null) {
                return skip(target, RepairOutcome.PLAN_IN_PROGRESS,
                        "active injection plan " + target.activePlanId());
            }
            if (now < target.nextEligibleAtMs()) {
                return skip(target, RepairOutcome.BACKOFF_SKIPPED,
                        "backoff until " + target.nextEligibleAtMs() + " (now=" + now + ")");
            }
            target.beginRepair();
            snapshot = target.snapshot();
        }

        // ---- 锁外：执行修复动作 + 健康校验 ----
        RepairOutcome failureOutcome = null;
        String failureDetail = null;
        boolean actionOk = false;
        try {
            try {
                executor.runWithTimeout("repair:" + targetId,
                        policy.repairTimeoutMs(),
                        () -> repairAction.apply(target));
                actionOk = true;
            } catch (TimeoutRuntimeException e) {
                failureOutcome = RepairOutcome.TIMED_OUT;
                failureDetail = e.getMessage();
            } catch (RuntimeException e) {
                failureOutcome = RepairOutcome.FAILED;
                failureDetail = e.getClass().getSimpleName() + ": " + e.getMessage();
            }

            boolean healthy = false;
            if (actionOk) {
                try {
                    healthy = healthProbe.isHealthy(target);
                } catch (RuntimeException e) {
                    failureOutcome = RepairOutcome.HEALTH_CHECK_FAILED;
                    failureDetail = "probe error: " + e.getMessage();
                }
                if (failureOutcome == null && !healthy) {
                    failureOutcome = RepairOutcome.HEALTH_CHECK_FAILED;
                    failureDetail = "post-repair health probe returned false";
                }
            }
        } finally {
            // 占位必须在任何情况下释放；真正的终态在临界区 2 推进
        }

        // ---- 临界区 2：回滚 / 计数 / 退避 / 隔离 ----
        synchronized (registry.lockFor(targetId)) {
            try {
                if (failureOutcome == null) {
                    target.markHealthy();
                    auditLog.record(new AuditEvent(clock.nowMs(), targetId, null,
                            "REPAIR", RepairOutcome.REPAIRED.name(),
                            "health probe passed; failures reset to 0"));
                    return new RepairResult(targetId, RepairOutcome.REPAIRED, 0,
                            TargetState.HEALTHY, 0L,
                            "repair succeeded and health verified");
                }

                // 先回滚到执行前状态（回滚失败抛 RollbackException，绝不静默）
                try {
                    target.restore(snapshot);
                } catch (RuntimeException rb) {
                    throw new com.github.lisama569.autoremediationorchestratorjava.engine.RollbackException(
                            "rollback failed during repair of " + targetId, rb);
                }
                // 回滚之后再计数：恰好 +1，由目标锁保证不重不丢
                int failures = target.recordFailure();
                long at = clock.nowMs();
                if (failures >= policy.maxConsecutiveFailures()) {
                    target.quarantine();
                    auditLog.record(new AuditEvent(at, targetId, null,
                            "REPAIR", RepairOutcome.QUARANTINED.name(),
                            "consecutiveFailures=" + failures + ">="
                                    + policy.maxConsecutiveFailures()
                                    + "; rolled back; retries stopped; detail=" + failureDetail));
                    return new RepairResult(targetId, RepairOutcome.QUARANTINED,
                            failures, TargetState.QUARANTINED, 0L,
                            "quarantined after " + failures + " failures: " + failureDetail);
                }
                long backoff = backoffMs(failures);
                target.setBackoffUntil(at + backoff);
                auditLog.record(new AuditEvent(at, targetId, null,
                        "REPAIR", failureOutcome.name(),
                        "rolledBack=true consecutiveFailures=" + failures
                                + " backoffMs=" + backoff
                                + " nextEligibleAt=" + (at + backoff)
                                + " detail=" + failureDetail));
                return new RepairResult(targetId, failureOutcome, failures,
                        target.state(), at + backoff,
                        "attempt failed and rolled back: " + failureDetail);
            } finally {
                target.endRepair();
            }
        }
    }

    private RepairResult skip(Target target, RepairOutcome outcome, String reason) {
        auditLog.record(new AuditEvent(clock.nowMs(), target.id(), null,
                "REPAIR", outcome.name(), reason + " (not counted)"));
        return new RepairResult(target.id(), outcome, target.consecutiveFailures(),
                target.state(), target.nextEligibleAtMs(), reason);
    }

    private long backoffMs(int failures) {
        double scaled = policy.backoffBaseMs()
                * Math.pow(policy.backoffMultiplier(), failures - 1);
        if (scaled <= 0d || policy.backoffMaxMs() <= 0L) {
            return 0L;
        }
        return Math.min(policy.backoffMaxMs(), (long) scaled);
    }
}
