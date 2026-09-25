package com.github.lisama569.autoremediationorchestratorjava.domain;

/**
 * 被编排的目标实例：进程内状态模拟。
 *
 * <p>该类是目标状态机的聚合根。所有变更方法包级可见，且约定只允许在
 * {@code TargetRegistry.lockFor(id)} 返回的监视器锁内调用，从而保证
 * 并发触发、重复计数、隔离判定在同一目标上串行化、可复现。</p>
 */
public final class Target {

    private final String id;
    private TargetState state;
    /** 连续修复失败次数；成功修复或恢复后归零。 */
    private int consecutiveFailures;
    /** 进行中的故障注入计划 ID；无则为 null。 */
    private String activePlanId;
    /** 是否有修复动作正在执行（防止重复触发造成重复计数）。 */
    private boolean repairRunning;
    /** 退避窗口结束时间（epoch ms），此前触发修复将被跳过且不计数。 */
    private long nextEligibleAtMs;
    /** 当前生效故障标记（注入动作写入，修复动作清除）。 */
    private String activeFault;

    public Target(String id) {
        this.id = id;
        this.state = TargetState.HEALTHY;
    }

    public String id() {
        return id;
    }

    public TargetState state() {
        return state;
    }

    public int consecutiveFailures() {
        return consecutiveFailures;
    }

    public String activePlanId() {
        return activePlanId;
    }

    public boolean repairRunning() {
        return repairRunning;
    }

    public long nextEligibleAtMs() {
        return nextEligibleAtMs;
    }

    public String activeFault() {
        return activeFault;
    }

    // ---- 包级状态变更（持锁调用） ----

    public void markUnhealthy(String fault) {
        this.state = TargetState.UNHEALTHY;
        this.activeFault = fault;
    }

    public void markHealthy() {
        this.state = TargetState.HEALTHY;
        this.activeFault = null;
        this.consecutiveFailures = 0;
        this.nextEligibleAtMs = 0L;
    }

    public void quarantine() {
        this.state = TargetState.QUARANTINED;
        this.activeFault = null;
        this.activePlanId = null;
        this.repairRunning = false;
        this.nextEligibleAtMs = 0L;
    }

    public boolean beginPlan(String planId) {
        if (activePlanId != null || state == TargetState.QUARANTINED) {
            return false;
        }
        this.activePlanId = planId;
        return true;
    }

    public void endPlan() {
        this.activePlanId = null;
    }

    public boolean beginRepair() {
        if (repairRunning) {
            return false;
        }
        this.repairRunning = true;
        return true;
    }

    public void endRepair() {
        this.repairRunning = false;
    }

    /** 失败计数 +1 并返回新值（不会重复计数也不会丢失）。 */
    public int recordFailure() {
        return ++consecutiveFailures;
    }

    public void setBackoffUntil(long epochMs) {
        this.nextEligibleAtMs = epochMs;
    }

    // ---- 快照与回滚 ----

    /** 不可变快照：执行（注入/修复）前的完整状态。 */
    public record Snapshot(TargetState state,
                           int consecutiveFailures,
                           String activePlanId,
                           boolean repairRunning,
                           long nextEligibleAtMs,
                           String activeFault) {
    }

    public Snapshot snapshot() {
        return new Snapshot(state, consecutiveFailures, activePlanId,
                repairRunning, nextEligibleAtMs, activeFault);
    }

    public void restore(Snapshot s) {
        this.state = s.state();
        this.consecutiveFailures = s.consecutiveFailures();
        this.activePlanId = s.activePlanId();
        this.repairRunning = s.repairRunning();
        this.nextEligibleAtMs = s.nextEligibleAtMs();
        this.activeFault = s.activeFault();
    }
}
