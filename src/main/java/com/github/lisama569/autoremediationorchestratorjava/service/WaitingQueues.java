package com.github.lisama569.autoremediationorchestratorjava.service;

import com.github.lisama569.autoremediationorchestratorjava.model.FaultInjectionPlan;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 按目标管理的忙时等待队列（FIFO）。
 *
 * 每个目标一把锁、一个有界队列：入队容量判定与出队调度都在同一把目标锁内完成，
 * 并发提交下队列容量不会超卖，计划不会丢失。锁对象与队列容器分离，队列在
 * 排空后可安全移除，不影响其他线程已持有的锁。
 */
public class WaitingQueues {

    private final int capacityPerTarget;
    private final Map<String, Deque<String>> queues = new ConcurrentHashMap<>();
    private final Map<String, Object> locks = new ConcurrentHashMap<>();

    public WaitingQueues(int capacityPerTarget) {
        if (capacityPerTarget < 1) {
            throw new IllegalArgumentException("queueCapacity must be >= 1, got " + capacityPerTarget);
        }
        this.capacityPerTarget = capacityPerTarget;
    }

    /** 获取该目标的队列锁；所有针对该目标队列的判定/变更都必须在这把锁内进行。 */
    Object lockFor(String targetId) {
        return locks.computeIfAbsent(targetId, id -> new Object());
    }

    /** 容量上限。 */
    int capacity() {
        return capacityPerTarget;
    }

    /** 必须在目标锁内调用：尝试把计划追加到队尾；队列未满返回 true，已满返回 false（不留任何状态）。 */
    boolean enqueue(String targetId, FaultInjectionPlan plan) {
        Deque<String> queue = queues.computeIfAbsent(targetId, id -> new ArrayDeque<>());
        if (queue.size() >= capacityPerTarget) {
            return false;
        }
        queue.addLast(plan.planId());
        return true;
    }

    /** 必须在目标锁内调用：取出队首计划 ID（队列为空返回 null），排空后移除空队列。 */
    String poll(String targetId) {
        Deque<String> queue = queues.get(targetId);
        if (queue == null || queue.isEmpty()) {
            return null;
        }
        String planId = queue.pollFirst();
        if (queue.isEmpty()) {
            queues.remove(targetId, queue);
        }
        return planId;
    }

    /** 必须在目标锁内调用：把计划 ID 放回队首（仅用于极端竞态下不丢计划的保守回退）。 */
    void addFirst(String targetId, String planId) {
        queues.computeIfAbsent(targetId, id -> new ArrayDeque<>()).addFirst(planId);
    }

    /** 必须在目标锁内调用：返回当前队列长度。 */
    int size(String targetId) {
        Deque<String> queue = queues.get(targetId);
        return queue == null ? 0 : queue.size();
    }
}
