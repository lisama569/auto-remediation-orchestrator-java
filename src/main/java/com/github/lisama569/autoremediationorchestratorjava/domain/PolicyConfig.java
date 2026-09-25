package com.github.lisama569.autoremediationorchestratorjava.domain;

/**
 * 自动修复策略配置。
 *
 * @param maxConsecutiveFailures 连续失败达到该阈值后隔离目标
 * @param backoffBaseMs          首次失败后的基础退避时长
 * @param backoffMultiplier      退避倍数（按连续失败次数指数增长）
 * @param backoffMaxMs           退避时长上限
 * @param repairTimeoutMs        单次修复动作超时
 * @param injectionTimeoutMs     单次故障注入动作超时
 */
public record PolicyConfig(int maxConsecutiveFailures,
                           long backoffBaseMs,
                           double backoffMultiplier,
                           long backoffMaxMs,
                           long repairTimeoutMs,
                           long injectionTimeoutMs) {
}
