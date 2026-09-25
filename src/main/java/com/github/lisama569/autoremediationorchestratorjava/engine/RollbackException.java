package com.github.lisama569.autoremediationorchestratorjava.engine;

/** 回滚失败时抛出：回滚链路异常不允许被静默吞掉。 */
public class RollbackException extends RuntimeException {
    public RollbackException(String message, Throwable cause) {
        super(message, cause);
    }
}
