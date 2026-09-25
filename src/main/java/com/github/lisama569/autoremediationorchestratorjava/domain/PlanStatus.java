package com.github.lisama569.autoremediationorchestratorjava.domain;

/**
 * 故障注入计划生命周期状态。
 *
 * <pre>
 * PENDING ──> RUNNING ──> COMPLETED
 *                │
 *                └──────> FAILED
 * </pre>
 */
public enum PlanStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    FAILED
}
