package com.github.lisama569.autoremediationorchestratorjava.domain;

/**
 * 修复触发结果。
 */
public record RepairResult(String targetId,
                           RepairOutcome outcome,
                           int consecutiveFailures,
                           TargetState targetState,
                           long nextEligibleAtMs,
                           String detail) {
}
