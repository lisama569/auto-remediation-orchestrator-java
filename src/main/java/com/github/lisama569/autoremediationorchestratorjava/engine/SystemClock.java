package com.github.lisama569.autoremediationorchestratorjava.engine;

/** 系统墙钟时间源。 */
public final class SystemClock implements Clock {

    @Override
    public long nowMs() {
        return System.currentTimeMillis();
    }
}
