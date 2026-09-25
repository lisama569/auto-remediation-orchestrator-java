package com.github.lisama569.autoremediationorchestratorjava.domain;

/**
 * 维护窗口：绝对时间区间 [startEpochMs, endEpochMs)。
 * 区间左闭右开，判定完全由注入的 {@code Clock} 驱动，保证可复现。
 */
public record MaintenanceWindow(long startEpochMs, long endEpochMs) {

    public MaintenanceWindow {
        if (endEpochMs <= startEpochMs) {
            throw new IllegalArgumentException(
                    "maintenance window end must be after start: "
                            + startEpochMs + ".." + endEpochMs);
        }
    }

    /** 当前时间是否位于维护窗口内。 */
    public boolean contains(long nowEpochMs) {
        return nowEpochMs >= startEpochMs && nowEpochMs < endEpochMs;
    }
}
