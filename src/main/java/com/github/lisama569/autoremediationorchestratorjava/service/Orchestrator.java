package com.github.lisama569.autoremediationorchestratorjava.service;

import com.github.lisama569.autoremediationorchestratorjava.domain.AuditEvent;
import com.github.lisama569.autoremediationorchestratorjava.domain.FaultInjectionPlan;
import com.github.lisama569.autoremediationorchestratorjava.domain.FaultType;
import com.github.lisama569.autoremediationorchestratorjava.domain.MaintenanceWindow;
import com.github.lisama569.autoremediationorchestratorjava.domain.PolicyConfig;
import com.github.lisama569.autoremediationorchestratorjava.domain.RepairResult;
import com.github.lisama569.autoremediationorchestratorjava.domain.SubmitResult;
import com.github.lisama569.autoremediationorchestratorjava.domain.Target;
import com.github.lisama569.autoremediationorchestratorjava.domain.TargetState;
import com.github.lisama569.autoremediationorchestratorjava.engine.ActionExecutor;
import com.github.lisama569.autoremediationorchestratorjava.engine.AuditLog;
import com.github.lisama569.autoremediationorchestratorjava.engine.Clock;
import com.github.lisama569.autoremediationorchestratorjava.engine.HealthProbe;
import com.github.lisama569.autoremediationorchestratorjava.engine.RepairAction;
import com.github.lisama569.autoremediationorchestratorjava.engine.SystemClock;
import com.github.lisama569.autoremediationorchestratorjava.engine.TargetRegistry;

import java.util.Map;

/**
 * 编排门面：注册目标、下发计划、触发修复、确认隔离目标恢复。
 *
 * <p>内置的故障/修复动作只操作 {@link Target} 的进程内状态，不触碰真实
 * 系统资源。故障动作也可由测试替换以制造阻塞（超时）与异常（失败）。</p>
 */
public final class Orchestrator {

    /** 默认策略：3 次连续失败隔离，100ms 起步 2 倍退避、上限 5s，动作超时 2s。 */
    public static PolicyConfig defaultPolicy() {
        return new PolicyConfig(3, 100L, 2.0, 5_000L, 2_000L, 2_000L);
    }

    private final TargetRegistry registry;
    private final AuditLog auditLog;
    private final Clock clock;
    private final PolicyConfig policy;
    private final ActionExecutor executor;
    private final PlanService planService;
    private final RepairService repairService;

    public Orchestrator(PolicyConfig policy) {
        this(policy, new SystemClock(), defaultFaultActions(),
                Orchestrator::defaultRepair, defaultHealthProbe());
    }

    public Orchestrator(PolicyConfig policy,
                        Clock clock,
                        Map<String, com.github.lisama569.autoremediationorchestratorjava.engine.FaultAction> faultActions,
                        RepairAction repairAction,
                        HealthProbe healthProbe) {
        this.registry = new TargetRegistry();
        this.auditLog = new AuditLog();
        this.clock = clock;
        this.policy = policy;
        this.executor = new ActionExecutor();
        this.planService = new PlanService(registry, auditLog, clock, policy,
                faultActions, executor);
        this.repairService = new RepairService(registry, auditLog, clock, policy,
                repairAction, healthProbe, executor);
    }

    public static Map<String, com.github.lisama569.autoremediationorchestratorjava.engine.FaultAction> defaultFaultActions() {
        // 真实状态由 PlanService 在动作成功后写入 markUnhealthy；
        // 动作本身仅提供可被测试替换的注入点（默认立即成功）。
        return Map.of(
                FaultType.DISK_FULL.name(), t -> { },
                FaultType.NETWORK_BLACKHOLE.name(), t -> { },
                FaultType.CPU_BURN.name(), t -> { });
    }

    /** 默认修复：清除故障标记；之后由 HealthProbe 做最终确认。 */
    public static void defaultRepair(Target target) {
        target.markHealthy();
    }

    /** 默认探针：无生效故障即健康。 */
    public static HealthProbe defaultHealthProbe() {
        return target -> target.activeFault() == null
                && target.state() == TargetState.HEALTHY;
    }

    public Target registerTarget(String targetId) {
        return registry.register(targetId);
    }

    public Target getTarget(String targetId) {
        return registry.find(targetId).orElseThrow(
                () -> new IllegalArgumentException("unknown target: " + targetId));
    }

    public SubmitResult submitPlan(String planId,
                                   String targetId,
                                   FaultType faultType,
                                   long idempotencyKey,
                                   MaintenanceWindow window) {
        return planService.submit(new FaultInjectionPlan(
                planId, targetId, faultType, idempotencyKey, window));
    }

    public RepairResult repair(String targetId) {
        return repairService.triggerRepair(targetId);
    }

    /**
     * 确认被隔离目标已恢复：探针通过则脱离隔离、重新参与后续调度。
     *
     * @return true 表示恢复成功；false 表示目标未隔离或探针仍不通过
     */
    public boolean recover(String targetId) {
        Target target = getTarget(targetId);
        synchronized (registry.lockFor(targetId)) {
            if (target.state() != TargetState.QUARANTINED) {
                auditLog.record(new AuditEvent(clock.nowMs(), targetId, null,
                        "RECOVER", "REJECTED", "target not quarantined"));
                return false;
            }
            boolean healthy = target.activeFault() == null;
            if (!healthy) {
                auditLog.record(new AuditEvent(clock.nowMs(), targetId, null,
                        "RECOVER", "REJECTED", "fault still present"));
                return false;
            }
            target.markHealthy();
            auditLog.record(new AuditEvent(clock.nowMs(), targetId, null,
                    "RECOVER", "RECOVERED",
                    "quarantine lifted; target rejoins scheduling"));
            return true;
        }
    }

    public void shutdown() {
        executor.shutdown();
    }

    public TargetRegistry registry() {
        return registry;
    }

    public AuditLog auditLog() {
        return auditLog;
    }

    public PolicyConfig policy() {
        return policy;
    }
}
