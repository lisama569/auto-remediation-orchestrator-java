package com.github.lisama569.autoremediationorchestratorjava.domain;

/**
 * 单次修复触发的判定结果，终态须稳定且可区分。
 */
public enum RepairOutcome {
    /** 修复成功，目标恢复健康。 */
    REPAIRED,
    /** 修复动作执行失败（异常），已回滚。 */
    FAILED,
    /** 修复动作超时，已回滚。 */
    TIMED_OUT,
    /** 健康校验未通过，已回滚。 */
    HEALTH_CHECK_FAILED,
    /** 连续失败达到阈值，目标已被隔离。 */
    QUARANTINED,
    /** 目标已在隔离中，不再重试。 */
    ALREADY_QUARANTINED,
    /** 处于退避窗口内，本次不执行、不计数。 */
    BACKOFF_SKIPPED,
    /** 已有修复在执行中，本次不执行、不计数。 */
    ALREADY_RUNNING,
    /** 目标有进行中的故障注入计划，本次不执行、不计数。 */
    PLAN_IN_PROGRESS,
    /** 目标本就健康，无需修复，不计数。 */
    ALREADY_HEALTHY,
    /** 目标不存在。 */
    UNKNOWN_TARGET
}
