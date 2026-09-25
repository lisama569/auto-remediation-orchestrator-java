package com.github.lisama569.autoremediationorchestratorjava.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进程内模拟的修复动作执行器。
 * 行为可按目标编程，便于测试确定性地触发成功/失败/挂起/无效修复等场景。
 */
@Component
public class SimulatedRemediationAction {

    private static final Logger log = LoggerFactory.getLogger(SimulatedRemediationAction.class);

    public enum Behavior {
        /** 动作成功并修复目标（恢复健康、清除故障） */
        SUCCEED,
        /** 动作抛出异常（模拟修复动作本身失败） */
        FAIL,
        /** 动作挂起直到被中断（模拟超时） */
        HANG,
        /** 动作正常返回但未修复目标（模拟健康校验不通过） */
        NOOP
    }

    private final Map<String, Behavior> behaviors = new ConcurrentHashMap<>();
    private final TargetRegistry registry;

    public SimulatedRemediationAction(TargetRegistry registry) {
        this.registry = registry;
    }

    public void setBehavior(String targetId, Behavior behavior) {
        behaviors.put(targetId, behavior);
    }

    public void clearBehavior(String targetId) {
        behaviors.remove(targetId);
    }

    /** 执行一次修复动作；成功路径会把目标恢复为健康。 */
    public void execute(String targetId) {
        Behavior behavior = behaviors.getOrDefault(targetId, Behavior.SUCCEED);
        switch (behavior) {
            case SUCCEED -> {
                registry.update(targetId, t -> t.withHealth(com.github.lisama569.autoremediationorchestratorjava.model.TargetHealth.HEALTHY).withFault(null));
                log.info("[action] target={} behavior=SUCCEED 目标已恢复健康", targetId);
            }
            case FAIL -> throw new IllegalStateException("simulated remediation action failure on target=" + targetId);
            case HANG -> {
                try {
                    Thread.sleep(Long.MAX_VALUE);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("action interrupted (timeout) on target=" + targetId, e);
                }
            }
            case NOOP -> log.info("[action] target={} behavior=NOOP 动作返回但未修复目标", targetId);
        }
    }
}
