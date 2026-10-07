package com.github.lisama569.autoremediationorchestratorjava.model;

/** 计划下发被拒绝的可区分原因。 */
public enum RejectionReason {
    /** 目标已被隔离 */
    TARGET_QUARANTINED,
    /** 目标未处于维护窗口 */
    OUTSIDE_MAINTENANCE_WINDOW,
    /** 同一目标已有进行中的计划 */
    PLAN_CONFLICT,
    /** 相同幂等键的重复下发 */
    DUPLICATE_REQUEST,
    /** 声明忙时排队但目标等待队列已满 */
    QUEUE_FULL
}
