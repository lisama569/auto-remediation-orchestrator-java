package com.github.lisama569.autoremediationorchestratorjava.service;

import com.github.lisama569.autoremediationorchestratorjava.model.Target;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

/**
 * 目标状态的线程安全注册表（进程内模拟被编排实例）。
 * 所有状态变更通过 {@link #update} 以 CAS 方式提交，保证并发下不丢计数、不重计数。
 */
@Service
public class TargetRegistry {

    private final Map<String, Target> targets = new ConcurrentHashMap<>();

    public void register(Target target) {
        targets.put(target.targetId(), target);
    }

    public Optional<Target> get(String targetId) {
        return Optional.ofNullable(targets.get(targetId));
    }

    /** 原子地对目标应用变更并返回最新快照。 */
    public Target update(String targetId, UnaryOperator<Target> mutation) {
        return targets.compute(targetId, (id, current) -> {
            if (current == null) {
                throw new IllegalStateException("target not registered: " + id);
            }
            return mutation.apply(current);
        });
    }
}
