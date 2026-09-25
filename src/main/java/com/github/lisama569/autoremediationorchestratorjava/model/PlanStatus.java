package com.github.lisama569.autoremediationorchestratorjava.model;

/** 故障注入计划的终态/中间态。 */
public enum PlanStatus {
    PENDING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    ROLLED_BACK,
    REJECTED
}
