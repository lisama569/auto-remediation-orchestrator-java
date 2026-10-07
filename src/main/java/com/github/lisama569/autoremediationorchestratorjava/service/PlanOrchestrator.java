package com.github.lisama569.autoremediationorchestratorjava.service;

import com.github.lisama569.autoremediationorchestratorjava.model.FaultInjectionPlan;
import com.github.lisama569.autoremediationorchestratorjava.model.FaultType;
import com.github.lisama569.autoremediationorchestratorjava.model.PlanStatus;
import com.github.lisama569.autoremediationorchestratorjava.model.RemediationPolicy;
import com.github.lisama569.autoremediationorchestratorjava.model.RejectionReason;
import com.github.lisama569.autoremediationorchestratorjava.model.SubmissionResult;
import com.github.lisama569.autoremediationorchestratorjava.model.Target;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

/**
 * 故障注入计划的编排入口。
 *
 * 校验顺序固定（保证拒绝原因稳定可区分）：
 * 1. 重复下发（幂等键已存在）→ DUPLICATE_REQUEST
 * 2. 目标已隔离 → TARGET_QUARANTINED
 * 3. 未处于维护窗口 → OUTSIDE_MAINTENANCE_WINDOW
 * 4a. 不排队的提交：同一目标已有进行中的计划 → PLAN_CONFLICT（历史语义，保持不变）
 * 4b. 排队提交（queueIfBusy=true）：目标忙则进入该目标的 FIFO 等待队列；
 *     队列达到容量上限 → QUEUE_FULL（不占名额、不留残留状态）
 *
 * 通过后异步执行：注入故障 → 自动修复 → 推进到终态（SUCCEEDED / ROLLED_BACK / FAILED）。
 * 前一个计划到达终态后，在目标锁内按 FIFO 摘下一个排队计划，出队前按目标当下
 * 状态重新判定，失效计划落入稳定的 REJECTED 终态，有效计划自动接续执行；
 * 同一目标任何时刻最多只有一个计划在执行。
 */
@Service
public class PlanOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(PlanOrchestrator.class);

    /** 每个目标等待队列的默认容量上限（可用构造参数 / 配置项覆盖）。 */
    public static final int DEFAULT_QUEUE_CAPACITY = 10;

    private final TargetRegistry registry;
    private final RemediationEngine engine;
    private final RemediationPolicy policy;
    private final int queueCapacity;
    private final ExecutorService executor = Executors.newFixedThreadPool(8);

    private final Map<String, FaultInjectionPlan> plans = new ConcurrentHashMap<>();
    /** 幂等键 → 已接受计划的 planId（一经接受即永久登记，覆盖"排队—执行—完成"全过程） */
    private final Map<String, String> idempotencyIndex = new ConcurrentHashMap<>();
    /** 目标 → 进行中计划的 planId（同一目标同一时刻只允许一个） */
    private final Map<String, String> activePlans = new ConcurrentHashMap<>();
    /** 按目标的忙时等待队列 */
    private final WaitingQueues waitingQueues;

    @org.springframework.beans.factory.annotation.Autowired
    public PlanOrchestrator(TargetRegistry registry, RemediationEngine engine,
                            @Value("${orchestrator.queue-capacity:" + DEFAULT_QUEUE_CAPACITY + "}") int queueCapacity) {
        this(registry, engine, RemediationPolicy.defaultPolicy(), queueCapacity);
    }

    public PlanOrchestrator(TargetRegistry registry, RemediationEngine engine, RemediationPolicy policy) {
        this(registry, engine, policy, DEFAULT_QUEUE_CAPACITY);
    }

    public PlanOrchestrator(TargetRegistry registry, RemediationEngine engine,
                            RemediationPolicy policy, int queueCapacity) {
        this.registry = registry;
        this.engine = engine;
        this.policy = policy;
        this.queueCapacity = queueCapacity;
        this.waitingQueues = new WaitingQueues(queueCapacity);
    }

    /** 不声明忙时排队的提交：行为与历史完全一致，目标忙时直接拒绝。 */
    public SubmissionResult submit(String targetId, FaultType faultType, String idempotencyKey) {
        return submit(targetId, faultType, idempotencyKey, false);
    }

    /**
     * 提交计划。
     *
     * @param queueIfBusy true 时，目标忙则进入该目标的 FIFO 等待队列（受容量上限约束）；
     *                    false 时保持历史语义，忙即拒绝。
     */
    public SubmissionResult submit(String targetId, FaultType faultType, String idempotencyKey,
                                   boolean queueIfBusy) {
        Target target = registry.get(targetId)
                .orElseThrow(() -> new IllegalArgumentException("target not registered: " + targetId));

        // 针对同一目标的判定全部串行化：队列容量、活动占位、出队调度共享同一把目标锁，
        // 并发提交下容量不会超卖、计划不会丢失、FIFO 顺序就是拿锁顺序。
        synchronized (waitingQueues.lockFor(targetId)) {
            // 1. 重复下发：返回已存在的计划，不造成重复执行（排队中/执行中/已终态均同）
            String existingPlanId = idempotencyIndex.get(idempotencyKey);
            if (existingPlanId != null) {
                FaultInjectionPlan existing = plans.get(existingPlanId);
                log.info("[submit] target={} key={} 判定=DUPLICATE_REQUEST, 已存在计划plan={} status={}",
                        targetId, idempotencyKey, existingPlanId, existing.status());
                return SubmissionResult.rejected(existing, RejectionReason.DUPLICATE_REQUEST);
            }

            FaultInjectionPlan plan = FaultInjectionPlan.newPlan(targetId, faultType, idempotencyKey);

            // 2. 目标已隔离
            if (target.quarantined()) {
                return reject(plan, RejectionReason.TARGET_QUARANTINED,
                        "目标已隔离, consecutiveFailures=" + target.consecutiveFailures());
            }
            // 3. 未处于维护窗口
            if (!target.inMaintenanceWindow()) {
                return reject(plan, RejectionReason.OUTSIDE_MAINTENANCE_WINDOW, "目标未处于维护窗口");
            }

            boolean busy = activePlans.get(targetId) != null;
            if (busy) {
                if (!queueIfBusy) {
                    // 4a. 历史语义：忙即拒绝
                    return reject(plan, RejectionReason.PLAN_CONFLICT,
                            "目标已有进行中的计划plan=" + activePlans.get(targetId));
                }
                // 4b. 排队提交：先判容量（此时尚未登记幂等键），满则直接拒绝，不占名额不留状态
                if (waitingQueues.size(targetId) >= queueCapacity) {
                    FaultInjectionPlan rejected = plan.rejected(RejectionReason.QUEUE_FULL);
                    log.info("[submit] plan={} target={} key={} 判定=QUEUE_FULL 依据: 等待队列已达上限{}，未入队不残留状态",
                            plan.planId(), targetId, idempotencyKey, queueCapacity);
                    return SubmissionResult.rejected(rejected, RejectionReason.QUEUE_FULL);
                }
                // 幂等键原子登记：跨目标同键竞争失败者按重复处理（此时尚未入队，无队列残留）
                if (idempotencyIndex.putIfAbsent(idempotencyKey, plan.planId()) != null) {
                    FaultInjectionPlan winner = plans.get(idempotencyIndex.get(idempotencyKey));
                    log.info("[submit] plan={} target={} key={} 判定=DUPLICATE_REQUEST(并发竞争, 未入队)",
                            plan.planId(), targetId, idempotencyKey);
                    return SubmissionResult.rejected(winner, RejectionReason.DUPLICATE_REQUEST);
                }
                waitingQueues.enqueue(targetId, plan); // 锁内容量已判，入队必成功
                FaultInjectionPlan queued = plan.withStatus(PlanStatus.QUEUED);
                plans.put(queued.planId(), queued);
                log.info("[submit] plan={} target={} fault={} key={} 判定=QUEUED 依据: 目标忙(执行中plan={}), 排队位置={}/{}",
                        plan.planId(), targetId, faultType, idempotencyKey,
                        activePlans.get(targetId), waitingQueues.size(targetId), queueCapacity);
                return SubmissionResult.accepted(queued);
            }

            // 目标空闲：登记幂等键 → 占位 → 异步执行。
            // 先登记键：跨目标同键竞争失败者直接按重复处理，无需回滚目标侧占位。
            if (idempotencyIndex.putIfAbsent(idempotencyKey, plan.planId()) != null) {
                FaultInjectionPlan winner = plans.get(idempotencyIndex.get(idempotencyKey));
                log.info("[submit] plan={} target={} key={} 判定=DUPLICATE_REQUEST(并发竞争)",
                        plan.planId(), targetId, idempotencyKey);
                return SubmissionResult.rejected(winner, RejectionReason.DUPLICATE_REQUEST);
            }
            String previous = activePlans.putIfAbsent(targetId, plan.planId());
            if (previous != null) {
                // 理论上不可达：活动占位的变更与本判定同在目标锁内。保守按冲突处理。
                return reject(plan, RejectionReason.PLAN_CONFLICT, "目标已有进行中的计划plan=" + previous);
            }
            plans.put(plan.planId(), plan);
            log.info("[submit] plan={} target={} fault={} key={} 判定=ACCEPTED",
                    plan.planId(), targetId, faultType, idempotencyKey);
            dispatch(plan.planId());
            return SubmissionResult.accepted(plan);
        }
    }

    private SubmissionResult reject(FaultInjectionPlan plan, RejectionReason reason, String detail) {
        FaultInjectionPlan rejected = plan.rejected(reason);
        plans.put(rejected.planId(), rejected);
        log.info("[submit] plan={} target={} 判定={} 依据: {}", plan.planId(), plan.targetId(), reason, detail);
        return SubmissionResult.rejected(rejected, reason);
    }

    private void dispatch(String planId) {
        try {
            executor.submit(() -> execute(planId));
        } catch (RejectedExecutionException e) {
            // 派发线程池饱和时退化为当前线程内联执行，保证已出队计划不会丢失
            log.warn("[dispatch] plan={} 异步派发被拒({}), 改为当前线程内联执行", planId, e.toString());
            execute(planId);
        }
    }

    /** 执行链路：注入故障 → 修复 → 推进终态。任何异常都记录并落为 FAILED，不静默吞掉。 */
    private void execute(String planId) {
        FaultInjectionPlan plan = plans.get(planId);
        String targetId = plan.targetId();
        try {
            transition(planId, PlanStatus.RUNNING);
            RemediationEngine.RemediationOutcome outcome = engine.remediate(plan, policy);
            PlanStatus terminal = outcome == RemediationEngine.RemediationOutcome.SUCCESS
                    ? PlanStatus.SUCCEEDED : PlanStatus.ROLLED_BACK;
            transition(planId, terminal);
            Target after = registry.get(targetId).orElseThrow();
            log.info("[execute] plan={} target={} 终态={} 判定依据: 修复结果={}, 目标health={} quarantined={} consecutiveFailures={}",
                    planId, targetId, terminal, outcome, after.health(), after.quarantined(), after.consecutiveFailures());
        } catch (Exception e) {
            log.error("[execute] plan={} target={} 执行链路异常, 终态=FAILED: {}", planId, targetId, e.toString());
            transition(planId, PlanStatus.FAILED);
        } finally {
            finishAndDispatchNext(targetId, planId);
        }
    }

    /**
     * 前一个计划到达终态后的收尾与接续调度（全部在目标锁内）：
     * 释放活动占位 → 逐个出队并重新判定 → 有效计划原子占位后异步执行。
     */
    private void finishAndDispatchNext(String targetId, String finishedPlanId) {
        String nextPlanId;
        synchronized (waitingQueues.lockFor(targetId)) {
            activePlans.remove(targetId, finishedPlanId);
            nextPlanId = revalidateNext(targetId);
            if (nextPlanId != null && activePlans.putIfAbsent(targetId, nextPlanId) != null) {
                // 不可达：占位刚在同一把锁内被释放。保守放弃本次调度并让计划回到队首。
                log.error("[dispatch] plan={} target={} 占位意外被占，计划重新排队首", nextPlanId, targetId);
                waitingQueues.addFirst(targetId, nextPlanId);
                nextPlanId = null;
            }
        }
        if (nextPlanId != null) {
            log.info("[dispatch] target={} 前序计划plan={}已终态, 自动接续执行plan={}",
                    targetId, finishedPlanId, nextPlanId);
            dispatch(nextPlanId);
        }
    }

    /**
     * 出队前按目标当下状态重新判定，返回第一个仍可执行的计划 ID（队列空返回 null）。
     * 失效计划不挂起、不丢弃，立刻落入带明确原因的 REJECTED 终态，然后继续检查后续计划。
     * 必须在目标锁内调用。
     */
    private String revalidateNext(String targetId) {
        while (true) {
            String nextId = waitingQueues.poll(targetId);
            if (nextId == null) {
                return null;
            }
            FaultInjectionPlan next = plans.get(nextId);
            RejectionReason invalid = null;
            // 与提交时一致的判定顺序：幂等 → 隔离 → 维护窗口
            if (!nextId.equals(idempotencyIndex.get(next.idempotencyKey()))) {
                invalid = RejectionReason.DUPLICATE_REQUEST;
            } else {
                Target current = registry.get(targetId)
                        .orElseThrow(() -> new IllegalStateException("target not registered: " + targetId));
                if (current.quarantined()) {
                    invalid = RejectionReason.TARGET_QUARANTINED;
                } else if (!current.inMaintenanceWindow()) {
                    invalid = RejectionReason.OUTSIDE_MAINTENANCE_WINDOW;
                }
            }
            if (invalid != null) {
                FaultInjectionPlan dead = next.rejected(invalid);
                plans.put(nextId, dead);
                log.info("[dispatch] plan={} target={} 排队失效 判定={} 依据: 出队重判目标当下状态, 剩余排队={}",
                        nextId, targetId, invalid, waitingQueues.size(targetId));
                continue;
            }
            log.info("[dispatch] plan={} target={} 出队重判通过, 开始执行, 剩余排队={}",
                    nextId, targetId, waitingQueues.size(targetId));
            return nextId;
        }
    }

    private void transition(String planId, PlanStatus status) {
        plans.computeIfPresent(planId, (id, p) -> p.withStatus(status));
    }

    public Optional<FaultInjectionPlan> getPlan(String planId) {
        return Optional.ofNullable(plans.get(planId));
    }

    /** 查询某目标等待队列当前长度（测试/可观测性用）。 */
    public int queueSize(String targetId) {
        synchronized (waitingQueues.lockFor(targetId)) {
            return waitingQueues.size(targetId);
        }
    }

    /** 恢复被隔离的目标：解除隔离并清零连续失败计数，使其重新参与后续调度。 */
    public Target recoverTarget(String targetId) {
        Target recovered = registry.update(targetId, Target::release);
        log.info("[recover] target={} 已解除隔离, quarantined={} consecutiveFailures={}",
                targetId, recovered.quarantined(), recovered.consecutiveFailures());
        return recovered;
    }
}
