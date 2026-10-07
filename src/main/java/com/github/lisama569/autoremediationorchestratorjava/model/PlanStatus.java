package com.github.lisama569.autoremediationorchestratorjava.model;

/** 故障注入计划的终态/中间态。 */
public enum PlanStatus {
    PENDING,
    /** 已被接受，正在目标的等待队列中排队，尚未开始执行 */
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    ROLLED_BACK,
    REJECTED
}
