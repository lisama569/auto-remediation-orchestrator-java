package com.github.lisama569.autoremediationorchestratorjava.engine;

/** 时间源抽象，便于测试中控制退避与超时判定。 */
public interface Clock {
    long nowMs();
}
