package com.github.lisama569.autoremediationorchestratorjava.engine;

/** 动作执行超时（可与动作自身异常区分）。 */
public class TimeoutRuntimeException extends RuntimeException {
    public TimeoutRuntimeException(String message) {
        super(message);
    }
}
