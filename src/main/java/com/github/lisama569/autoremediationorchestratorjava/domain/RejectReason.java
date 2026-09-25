package com.github.lisama569.autoremediationorchestratorjava.domain;

/**
 * 计划下发被拒绝时的可区分原因。
 */
public enum RejectReason {

    /** 当前时间不在维护窗口内。 */
    OUTSIDE_MAINTENANCE_WINDOW,

    /** 目标已被隔离，禁止下发故障计划。 */
    TARGET_QUARANTINED,

    /** 同一目标已有进行中的计划。 */
    PLAN_ALREADY_IN_PROGRESS,

    /** 幂等键重复：同一请求被重复下发。 */
    DUPLICATE_REQUEST
}
