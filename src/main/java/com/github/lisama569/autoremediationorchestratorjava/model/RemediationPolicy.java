package com.github.lisama569.autoremediationorchestratorjava.model;

/**
 * 自动修复策略。
 *
 * @param maxRetries          单次修复动作的最大重试次数（不含首次执行）
 * @param backoffMillis       重试退避基数（毫秒），按尝试次数线性增长
 * @param quarantineThreshold 连续失败达到该次数后目标进入隔离
 * @param actionTimeoutMillis 单次修复动作的超时时间（毫秒）
 * @param queueCapacity       每个目标等待队列的容量上限（不含正在执行的计划）
 */
public record RemediationPolicy(
        int maxRetries,
        long backoffMillis,
        int quarantineThreshold,
        long actionTimeoutMillis,
        int queueCapacity) {

    /** 兼容旧调用方：不显式指定队列容量时使用默认值。 */
    public RemediationPolicy(int maxRetries, long backoffMillis, int quarantineThreshold, long actionTimeoutMillis) {
        this(maxRetries, backoffMillis, quarantineThreshold, actionTimeoutMillis, DEFAULT_QUEUE_CAPACITY);
    }

    public static final int DEFAULT_QUEUE_CAPACITY = 8;

    public static RemediationPolicy defaultPolicy() {
        return new RemediationPolicy(2, 50, 3, 500);
    }
}
