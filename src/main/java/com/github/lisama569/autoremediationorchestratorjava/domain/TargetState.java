package com.github.lisama569.autoremediationorchestratorjava.domain;

/**
 * 被编排目标（进程内模拟实例）的状态。
 *
 * <pre>
 * HEALTHY ──注入故障──> UNHEALTHY
 *    ▲                    │
 *    │修复成功            │修复连续失败达到阈值
 *    │                    ▼
 *    └────────恢复──── QUARANTINED
 * </pre>
 */
public enum TargetState {
    HEALTHY,
    UNHEALTHY,
    QUARANTINED
}
