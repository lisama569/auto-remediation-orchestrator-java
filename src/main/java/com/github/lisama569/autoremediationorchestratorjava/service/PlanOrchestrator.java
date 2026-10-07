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
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 故障注入计划的编排入口。
 *
 * 校验顺序固定（保证拒绝原因稳定可区分）：
 * 1. 重复下发（幂等键已存在）→ DUPLICATE_REQUEST
 * 2. 目标已隔离 → TARGET_QUARANTINED
 * 3. 未处于维护窗口 → OUTSIDE_MAINTENANCE_WINDOW
 * 4. 同一目标已有进行中的计划：
 *    - 未声明忙时排队 → PLAN_CONFLICT（原行为不变）
 *    - 声明忙时排队且队列未满 → 进入目标等待队列（QUEUED），按提交先后接续执行
 *    - 声明忙时排队但队列已满 → QUEUE_FULL（不留任何残留状态）
 *
 * 通过后异步执行：注入故障 → 自动修复 → 推进到终态（SUCCEEDED / ROLLED_BACK / FAILED）。
 * 前序计划到达终态后，在等待队列内按 FIFO 提升下一个计划；提升前按目标当下状态
 * 重新判定（隔离/维护窗口/幂等键），不再可执行的计划落到 REJECTED 终态并附带原因。
 *
 * 同一目标的"执行位占用 / 入队 / 提升"全部在该目标的队列锁内完成，
 * 保证任何时刻同一目标只有一个计划在执行，且队列容量不超卖、计划不丢失。
 */
@Service
public class PlanOrchestrator {

    private static final Logger log = LoggerFactory.getLogger(PlanOrchestrator.class);

    private final TargetRegistry registry;
    private final RemediationEngine engine;
    private final RemediationPolicy policy;
    private final ExecutorService executor = Executors.newFixedThreadPool(8);

    private final Map<String, FaultInjectionPlan> plans = new ConcurrentHashMap<>();
    /** 幂等键 → 已接受计划的 planId */
    private final Map<String, String> idempotencyIndex = new ConcurrentHashMap<>();
    /** 目标 → 进行中计划的 planId（同一目标同一时刻只允许一个） */
    private final Map<String, String> activePlans = new ConcurrentHashMap<>();
    /** 目标 → 等待队列（QUEUED 计划的 planId，FIFO）。作为该目标调度决策的锁对象使用。 */
    private final Map<String, ArrayDeque<String>> queues = new ConcurrentHashMap<>();

    @org.springframework.beans.factory.annotation.Autowired
    public PlanOrchestrator(TargetRegistry registry, RemediationEngine engine) {
        this(registry, engine, RemediationPolicy.defaultPolicy());
    }

    public PlanOrchestrator(TargetRegistry registry, RemediationEngine engine, RemediationPolicy policy) {
        this.registry = registry;
        this.engine = engine;
        this.policy = policy;
    }

    public SubmissionResult submit(String targetId, FaultType faultType, String idempotencyKey) {
        return submit(targetId, faultType, idempotencyKey, false);
    }

    /**
     * 下发计划。queueWhenBusy=true 时，若目标正忙则进入该目标的等待队列（容量有限），
     * 前序计划到达终态后按提交先后自动接续执行；否则行为与原提交完全一致。
     */
    public SubmissionResult submit(String targetId, FaultType faultType, String idempotencyKey,
                                   boolean queueWhenBusy) {
        Target target = registry.get(targetId)
                .orElseThrow(() -> new IllegalArgumentException("target not registered: " + targetId));

        // 1. 重复下发：返回已存在的计划，不造成重复执行
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

        // 4. 执行位占用 / 入队决策：与提升（promote）在同一把目标队列锁内完成，保证不超卖、不丢失
        ArrayDeque<String> queue = queues.computeIfAbsent(targetId, k -> new ArrayDeque<>());
        synchronized (queue) {
            if (activePlans.putIfAbsent(targetId, plan.planId()) == null) {
                // 抢到执行位：幂等键原子登记；并发下同键竞争失败者释放占位并按重复处理
                if (idempotencyIndex.putIfAbsent(idempotencyKey, plan.planId()) != null) {
                    activePlans.remove(targetId, plan.planId());
                    FaultInjectionPlan existing = plans.get(idempotencyIndex.get(idempotencyKey));
                    log.info("[submit] plan={} target={} key={} 判定=DUPLICATE_REQUEST(并发竞争)",
                            plan.planId(), targetId, idempotencyKey);
                    return SubmissionResult.rejected(existing, RejectionReason.DUPLICATE_REQUEST);
                }
                plans.put(plan.planId(), plan);
                log.info("[submit] plan={} target={} fault={} key={} 判定=ACCEPTED",
                        plan.planId(), targetId, faultType, idempotencyKey);
                executor.submit(() -> execute(plan.planId()));
                return SubmissionResult.accepted(plan);
            }

            // 目标忙：未声明排队 → 与原行为一致的 PLAN_CONFLICT
            if (!queueWhenBusy) {
                return reject(plan, RejectionReason.PLAN_CONFLICT,
                        "目标已有进行中的计划plan=" + activePlans.get(targetId));
            }
            // 锁内复查幂等键：排队期间同键可能已被其他请求登记
            String racedExisting = idempotencyIndex.get(idempotencyKey);
            if (racedExisting != null) {
                log.info("[submit] plan={} target={} key={} 判定=DUPLICATE_REQUEST(入队前复查), 已存在计划plan={}",
                        plan.planId(), targetId, idempotencyKey, racedExisting);
                return SubmissionResult.rejected(plans.get(racedExisting), RejectionReason.DUPLICATE_REQUEST);
            }
            // 队列容量上限：队满拒绝不登记幂等键、不落计划存储，不留任何残留状态
            if (queue.size() >= policy.queueCapacity()) {
                FaultInjectionPlan rejected = plan.rejected(RejectionReason.QUEUE_FULL);
                log.info("[submit] plan={} target={} 判定=QUEUE_FULL 依据: 等待队列已满 {}/{}",
                        plan.planId(), targetId, queue.size(), policy.queueCapacity());
                return SubmissionResult.rejected(rejected, RejectionReason.QUEUE_FULL);
            }
            // 入队：原子登记幂等键（跨目标同键竞争的最终防线），失败则不占队列名额
            if (idempotencyIndex.putIfAbsent(idempotencyKey, plan.planId()) != null) {
                FaultInjectionPlan existing = plans.get(idempotencyIndex.get(idempotencyKey));
                log.info("[submit] plan={} target={} key={} 判定=DUPLICATE_REQUEST(入队竞争)",
                        plan.planId(), targetId, idempotencyKey);
                return SubmissionResult.rejected(existing, RejectionReason.DUPLICATE_REQUEST);
            }
            FaultInjectionPlan queued = plan.withStatus(PlanStatus.QUEUED);
            plans.put(queued.planId(), queued);
            queue.addLast(queued.planId());
            log.info("[submit] plan={} target={} fault={} key={} 判定=QUEUED, 队列位置={}/{}",
                    queued.planId(), targetId, faultType, idempotencyKey, queue.size(), policy.queueCapacity());
            return SubmissionResult.accepted(queued);
        }
    }

    private SubmissionResult reject(FaultInjectionPlan plan, RejectionReason reason, String detail) {
        FaultInjectionPlan rejected = plan.rejected(reason);
        plans.put(rejected.planId(), rejected);
        log.info("[submit] plan={} target={} 判定={} 依据: {}", plan.planId(), plan.targetId(), reason, detail);
        return SubmissionResult.rejected(rejected, reason);
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
            // 释放执行位并提升等待队列中的下一个计划：与 submit 的占位/入队在同一把锁内，
            // 保证同一目标任一时刻只有一个计划在执行，且提升不被新提交插队。
            ArrayDeque<String> queue = queues.computeIfAbsent(targetId, k -> new ArrayDeque<>());
            synchronized (queue) {
                activePlans.remove(targetId, planId);
                promoteNextLocked(targetId, queue);
            }
        }
    }

    /**
     * 在目标队列锁内按 FIFO 提升等待中的计划。
     * 提升前按目标当下状态重新判定：幂等键已被其他请求完成、目标被隔离、维护窗口已关闭
     * 的计划落到 REJECTED 终态（附带可区分原因），不挂着不管、不悄悄丢弃；
     * 遇到第一个仍可执行的计划即占位并启动。
     */
    private void promoteNextLocked(String targetId, ArrayDeque<String> queue) {
        String nextId;
        while ((nextId = queue.pollFirst()) != null) {
            FaultInjectionPlan next = plans.get(nextId);
            if (next == null) {
                continue; // 防御：队列引用与计划存储不一致时跳过，不中断后续提升
            }
            Target target = registry.get(targetId).orElseThrow();
            RejectionReason invalid = null;
            String basis = null;
            if (!nextId.equals(idempotencyIndex.get(next.idempotencyKey()))) {
                invalid = RejectionReason.DUPLICATE_REQUEST;
                basis = "同一幂等键已由其他请求先完成, key=" + next.idempotencyKey();
            } else if (target.quarantined()) {
                invalid = RejectionReason.TARGET_QUARANTINED;
                basis = "排队期间目标被隔离, consecutiveFailures=" + target.consecutiveFailures();
            } else if (!target.inMaintenanceWindow()) {
                invalid = RejectionReason.OUTSIDE_MAINTENANCE_WINDOW;
                basis = "排队期间维护窗口已关闭";
            }
            if (invalid != null) {
                plans.put(nextId, next.rejected(invalid));
                log.info("[promote] plan={} target={} 判定={} 依据: {}", nextId, targetId, invalid, basis);
                continue;
            }
            activePlans.put(targetId, nextId);
            log.info("[promote] plan={} target={} 判定=PROMOTED 依据: 前序计划已到终态, 重新判定通过"
                    + "(quarantined={} inMaintenanceWindow={}), 开始执行",
                    nextId, targetId, target.quarantined(), target.inMaintenanceWindow());
            String promotedId = nextId;
            executor.submit(() -> execute(promotedId));
            return;
        }
    }

    private void transition(String planId, PlanStatus status) {
        plans.computeIfPresent(planId, (id, p) -> p.withStatus(status));
    }

    public Optional<FaultInjectionPlan> getPlan(String planId) {
        return Optional.ofNullable(plans.get(planId));
    }

    /** 恢复被隔离的目标：解除隔离并清零连续失败计数，使其重新参与后续调度。 */
    public Target recoverTarget(String targetId) {
        Target recovered = registry.update(targetId, Target::release);
        log.info("[recover] target={} 已解除隔离, quarantined={} consecutiveFailures={}",
                targetId, recovered.quarantined(), recovered.consecutiveFailures());
        return recovered;
    }
}
