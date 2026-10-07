package com.github.lisama569.autoremediationorchestratorjava.model;

/** 故障注入计划的终态/中间态。 */
public enum PlanStatus {
    PENDING,
    /** 已声明忙时排队，正在目标等待队列中等待前序计划到达终态 */
    QUEUED,
    RUNNING,
    SUCCEEDED,
    FAILED,
    ROLLED_BACK,
    REJECTED
}
