package com.github.lisama569.autoremediationorchestratorjava.tests;

import com.github.lisama569.autoremediationorchestratorjava.engine.Clock;

/** 可控时间源：测试手动推进时间，使维护窗口与退避判定完全确定。 */
public final class MutableClock implements Clock {

    private long now;

    public MutableClock(long start) {
        this.now = start;
    }

    @Override
    public long nowMs() {
        return now;
    }

    public void advance(long deltaMs) {
        this.now += deltaMs;
    }
}
