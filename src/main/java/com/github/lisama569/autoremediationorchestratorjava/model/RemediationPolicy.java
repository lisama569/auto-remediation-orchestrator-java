package com.github.lisama569.autoremediationorchestratorjava.model;

/**
 * 自动修复策略。
 *
 * @param maxRetries          单次修复动作的最大重试次数（不含首次执行）
 * @param backoffMillis       重试退避基数（毫秒），按尝试次数线性增长
 * @param quarantineThreshold 连续失败达到该次数后目标进入隔离
 * @param actionTimeoutMillis 单次修复动作的超时时间（毫秒）
 */
public record RemediationPolicy(
        int maxRetries,
        long backoffMillis,
        int quarantineThreshold,
        long actionTimeoutMillis) {

    public static RemediationPolicy defaultPolicy() {
        return new RemediationPolicy(2, 50, 3, 500);
    }
}
