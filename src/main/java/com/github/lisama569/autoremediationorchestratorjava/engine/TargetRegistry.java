package com.github.lisama569.autoremediationorchestratorjava.engine;

import com.github.lisama569.autoremediationorchestratorjava.domain.Target;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 目标注册表。
 *
 * <p>每个目标拥有一把独立监视器锁（{@link #lockFor(String)}），同一目标的
 * 计划下发、注入执行、修复触发、计数与隔离判定全部在该锁内串行完成，
 * 因此并发请求下不会重复计数或丢失；不同目标之间互不阻塞。</p>
 */
public final class TargetRegistry {

    private record Entry(Target target, Object lock) {
    }

    private final ConcurrentHashMap<String, Entry> entries = new ConcurrentHashMap<>();

    /** 注册目标；重复注册返回既有实例（幂等）。 */
    public Target register(String targetId) {
        return entries.computeIfAbsent(targetId, id -> new Entry(new Target(id), new Object())).target();
    }

    public Optional<Target> find(String targetId) {
        return Optional.ofNullable(entries.get(targetId)).map(Entry::target);
    }

    /** 返回该目标的专用锁；目标必须先经 {@link #register(String)} 注册。 */
    public Object lockFor(String targetId) {
        Entry entry = entries.get(targetId);
        if (entry == null) {
            throw new IllegalArgumentException("unknown target: " + targetId);
        }
        return entry.lock();
    }
}
