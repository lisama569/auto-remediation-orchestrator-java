package com.github.lisama569.autoremediationorchestratorjava.engine;

import com.github.lisama569.autoremediationorchestratorjava.domain.Target;

/**
 * 修复动作：对进程内模拟目标执行修复。
 * 抛异常=执行失败；阻塞超过超时=超时。
 */
@FunctionalInterface
public interface RepairAction {
    void apply(Target target) throws Exception;
}
