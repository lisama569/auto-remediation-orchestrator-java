package com.github.lisama569.autoremediationorchestratorjava.engine;

import com.github.lisama569.autoremediationorchestratorjava.domain.Target;

/**
 * 故障注入动作：对进程内模拟目标施加故障。
 * 实现可抛异常表示注入失败，可通过阻塞制造"进行中"窗口。
 */
@FunctionalInterface
public interface FaultAction {
    void apply(Target target) throws Exception;
}
