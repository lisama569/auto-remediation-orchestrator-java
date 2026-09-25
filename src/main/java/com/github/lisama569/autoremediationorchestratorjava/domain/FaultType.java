package com.github.lisama569.autoremediationorchestratorjava.domain;

/**
 * 支持的故障注入类型。
 */
public enum FaultType {
    /** 磁盘填满：占用磁盘资源。 */
    DISK_FULL,
    /** 网络不可达。 */
    NETWORK_BLACKHOLE,
    /** CPU 持续高负载。 */
    CPU_BURN
}
