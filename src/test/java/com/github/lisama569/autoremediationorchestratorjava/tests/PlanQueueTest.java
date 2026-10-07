package com.github.lisama569.autoremediationorchestratorjava.tests;

import com.github.lisama569.autoremediationorchestratorjava.model.FaultInjectionPlan;
import com.github.lisama569.autoremediationorchestratorjava.model.FaultType;
import com.github.lisama569.autoremediationorchestratorjava.model.PlanStatus;
import com.github.lisama569.autoremediationorchestratorjava.model.RemediationPolicy;
import com.github.lisama569.autoremediationorchestratorjava.model.RejectionReason;
import com.github.lisama569.autoremediationorchestratorjava.model.SubmissionResult;
import com.github.lisama569.autoremediationorchestratorjava.model.Target;
import com.github.lisama569.autoremediationorchestratorjava.model.TargetHealth;
import com.github.lisama569.autoremediationorchestratorjava.service.PlanOrchestrator;
import com.github.lisama569.autoremediationorchestratorjava.service.RemediationEngine;
import com.github.lisama569.autoremediationorchestratorjava.service.SimulatedRemediationAction;
import com.github.lisama569.autoremediationorchestratorjava.service.TargetRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 忙时排队与按目标容量控制：FIFO 接续执行、容量边界不超卖不丢计划、
 * 排队期间目标状态变化后的稳定收敛、排队阶段同幂等键重复提交。
 */
class PlanQueueTest {

    private static final Logger log = LoggerFactory.getLogger(PlanQueueTest.class);
    private static final String TARGET = "node-queue";
    private static final int QUEUE_CAPACITY = 2;

    private TargetRegistry registry;
    private GateAction action;
    private PlanOrchestrator orchestrator;

    /** 每次修复执行进入独立闸门，测试可按执行次序逐道放行，从而确定性地观察 RUNNING 与顺序。 */
    static final class GateAction extends SimulatedRemediationAction {
        private final TargetRegistry registry;
        private final CountDownLatch[] gates;
        private final AtomicInteger entered = new AtomicInteger();
        private final List<String> enteredOrder = Collections.synchronizedList(new ArrayList<>());

        GateAction(TargetRegistry registry, int maxExecutions) {
            super(registry);
            this.registry = registry;
            this.gates = new CountDownLatch[maxExecutions];
            for (int i = 0; i < maxExecutions; i++) {
                gates[i] = new CountDownLatch(1);
            }
        }

        void release(int index) {
            gates[index].countDown();
        }

        void releaseAll() {
            for (CountDownLatch gate : gates) {
                gate.countDown();
            }
        }

        int enteredCount() {
            return entered.get();
        }

        List<String> enteredOrder() {
            return enteredOrder;
        }

        @Override
        public void execute(String targetId) {
            int index = entered.getAndIncrement();
            enteredOrder.add(targetId + "#exec-" + index);
            try {
                gates[index].await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("gate wait interrupted", e);
            }
            registry.update(targetId, t -> t.withHealth(TargetHealth.HEALTHY).withFault(null));
        }
    }

    @BeforeEach
    void setUp() {
        registry = new TargetRegistry();
        action = new GateAction(registry, 32);
        RemediationEngine engine = new RemediationEngine(registry, action);
        // 闸门等待期间不允许引擎超时打断：动作超时给 5 秒
        orchestrator = new PlanOrchestrator(registry, engine,
                new RemediationPolicy(0, 10, 3, 5_000), QUEUE_CAPACITY);
        registry.register(Target.healthy(TARGET, true));
    }

    private FaultInjectionPlan awaitStatus(String planId, PlanStatus status, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        FaultInjectionPlan plan = null;
        while (System.nanoTime() < deadline) {
            plan = orchestrator.getPlan(planId).orElseThrow();
            if (plan.status() == status) {
                return plan;
            }
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        }
        throw new AssertionError("plan " + planId + " did not reach " + status + ", last="
                + (plan == null ? "null" : plan.status()));
    }

    @Test
    void busyTargetQueuesPlansAndExecutesInFifoOrderWithCapacityLimit() {
        // 第 1 个计划立即执行，占住目标（闸门 0 未放行）
        SubmissionResult first = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "q-first", true);
        assertTrue(first.accepted());
        awaitStatus(first.plan().planId(), PlanStatus.RUNNING, Duration.ofSeconds(5));

        // 之后两个排队提交进入队列（容量=2），外部查询一眼可见 QUEUED
        SubmissionResult second = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "q-second", true);
        SubmissionResult third = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "q-third", true);
        assertTrue(second.accepted());
        assertTrue(third.accepted());
        assertEquals(PlanStatus.QUEUED, orchestrator.getPlan(second.plan().planId()).orElseThrow().status());
        assertEquals(PlanStatus.QUEUED, orchestrator.getPlan(third.plan().planId()).orElseThrow().status());
        assertEquals(2, orchestrator.queueSize(TARGET));
        log.info("[test] target={} 排队就位: running={} queued=[{},{}] queueSize={}/{}",
                TARGET, first.plan().planId(), second.plan().planId(), third.plan().planId(),
                orchestrator.queueSize(TARGET), QUEUE_CAPACITY);

        // 队列已满：再来的排队提交必须以 QUEUE_FULL 被拒，且不占名额
        SubmissionResult overflow = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "q-overflow", true);
        assertEquals(false, overflow.accepted());
        assertEquals(RejectionReason.QUEUE_FULL, overflow.rejectionReason());
        assertEquals(PlanStatus.REJECTED, overflow.plan().status());
        assertEquals(2, orchestrator.queueSize(TARGET), "队满拒绝不得占用队列名额");
        assertTrue(orchestrator.getPlan(overflow.plan().planId()).isEmpty(),
                "被队满拒绝的计划不得在系统中留下可查询的残留状态");
        log.info("[test] target={} 队满提交plan={} 判定=QUEUE_FULL, queueSize仍={}, 无残留计划",
                TARGET, overflow.plan().planId(), orchestrator.queueSize(TARGET));

        // 同一时刻只有一个在执行：第二、三个计划都还没开始
        assertEquals(1, action.enteredCount());

        // 放行第 1 个执行 → 第 2 个自动接续为 RUNNING（FIFO）
        action.release(0);
        awaitStatus(first.plan().planId(), PlanStatus.SUCCEEDED, Duration.ofSeconds(5));
        awaitStatus(second.plan().planId(), PlanStatus.RUNNING, Duration.ofSeconds(5));
        assertEquals(1, orchestrator.queueSize(TARGET));
        assertEquals(PlanStatus.QUEUED, orchestrator.getPlan(third.plan().planId()).orElseThrow().status());
        log.info("[test] target={} plan={}完成, plan={}自动接续RUNNING, queueSize={}",
                TARGET, first.plan().planId(), second.plan().planId(), orchestrator.queueSize(TARGET));

        // 队列腾出 1 个名额后，新的排队提交必须能正常进来
        SubmissionResult fourth = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "q-fourth", true);
        assertTrue(fourth.accepted());
        assertEquals(PlanStatus.QUEUED, orchestrator.getPlan(fourth.plan().planId()).orElseThrow().status());
        assertEquals(2, orchestrator.queueSize(TARGET));
        log.info("[test] target={} 腾出名额后新计划plan={}判定=QUEUED, queueSize={}",
                TARGET, fourth.plan().planId(), orchestrator.queueSize(TARGET));

        // 依次放行，全部按提交先后 SUCCEEDED
        action.release(1);
        awaitStatus(second.plan().planId(), PlanStatus.SUCCEEDED, Duration.ofSeconds(5));
        awaitStatus(third.plan().planId(), PlanStatus.RUNNING, Duration.ofSeconds(5));
        action.release(2);
        awaitStatus(third.plan().planId(), PlanStatus.SUCCEEDED, Duration.ofSeconds(5));
        awaitStatus(fourth.plan().planId(), PlanStatus.RUNNING, Duration.ofSeconds(5));
        action.release(3);
        awaitStatus(fourth.plan().planId(), PlanStatus.SUCCEEDED, Duration.ofSeconds(5));

        assertEquals(0, orchestrator.queueSize(TARGET));
        assertEquals(List.of(TARGET + "#exec-0", TARGET + "#exec-1", TARGET + "#exec-2", TARGET + "#exec-3"),
                action.enteredOrder(), "排队计划必须严格按提交先后执行");
        log.info("[test] target={} FIFO执行次序={} queueSize={}",
                TARGET, action.enteredOrder(), orchestrator.queueSize(TARGET));
    }

    @Test
    void nonQueuedSubmitStillRejectsBusyTargetWithPlanConflict() {
        SubmissionResult first = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "nq-first", false);
        assertTrue(first.accepted());
        awaitStatus(first.plan().planId(), PlanStatus.RUNNING, Duration.ofSeconds(5));

        // 不声明排队的提交，忙时行为与历史完全一致：PLAN_CONFLICT，不进入队列
        SubmissionResult conflict = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "nq-second", false);
        assertEquals(RejectionReason.PLAN_CONFLICT, conflict.rejectionReason());
        assertEquals(0, orchestrator.queueSize(TARGET));
        log.info("[test] target={} 未声明排队的忙时提交plan={} 判定={} 依据=历史语义不变, queueSize={}",
                TARGET, conflict.plan().planId(), conflict.rejectionReason(), orchestrator.queueSize(TARGET));

        action.releaseAll();
        awaitStatus(first.plan().planId(), PlanStatus.SUCCEEDED, Duration.ofSeconds(5));
    }

    @Test
    void queuedPlanInvalidatedWhenTargetQuarantinedDuringWait() {
        SubmissionResult first = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "vq-first", true);
        awaitStatus(first.plan().planId(), PlanStatus.RUNNING, Duration.ofSeconds(5));
        SubmissionResult queued = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "vq-wait", true);
        assertTrue(queued.accepted());
        assertEquals(PlanStatus.QUEUED, orchestrator.getPlan(queued.plan().planId()).orElseThrow().status());

        // 排队期间目标被隔离：出队重判必须落入稳定、可区分的终态，而不是挂着或被静默执行
        registry.update(TARGET, Target::quarantine);
        log.info("[test] target={} plan={}排队期间目标被隔离", TARGET, queued.plan().planId());

        action.release(0);
        FaultInjectionPlan dead = TestAwait.awaitTerminal(
                orchestrator, queued.plan().planId(), Duration.ofSeconds(5));
        assertEquals(PlanStatus.REJECTED, dead.status());
        assertEquals(RejectionReason.TARGET_QUARANTINED, dead.rejectionReason());
        log.info("[test] target={} plan={} 终态={} 原因={} 依据=排队期间目标被隔离, 出队重判失效",
                TARGET, dead.planId(), dead.status(), dead.rejectionReason());

        // 隔离期间新的排队/非排队提交仍以 TARGET_QUARANTINED 拒绝，队列为空
        assertEquals(0, orchestrator.queueSize(TARGET));
        SubmissionResult whileIso = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "vq-after", true);
        assertEquals(RejectionReason.TARGET_QUARANTINED, whileIso.rejectionReason());

        // 恢复后重新参与调度，全新计划可正常执行
        orchestrator.recoverTarget(TARGET);
        SubmissionResult recoveredPlan = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "vq-recovered", true);
        assertTrue(recoveredPlan.accepted());
        action.releaseAll();
        FaultInjectionPlan terminal = TestAwait.awaitTerminal(
                orchestrator, recoveredPlan.plan().planId(), Duration.ofSeconds(10));
        assertEquals(PlanStatus.SUCCEEDED, terminal.status());
        log.info("[test] target={} 恢复后plan={}终态={}", TARGET, terminal.planId(), terminal.status());
    }

    @Test
    void queuedPlansInvalidatedWhenMaintenanceWindowClosesThenLaterPlanRuns() {
        SubmissionResult first = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "mw-first", true);
        awaitStatus(first.plan().planId(), PlanStatus.RUNNING, Duration.ofSeconds(5));
        // 队列容量 2：两个计划都在窗口关闭期间排队，出队重判时都应失效
        SubmissionResult blocked = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "mw-blocked", true);
        SubmissionResult blocked2 = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "mw-blocked2", true);
        assertTrue(blocked.accepted());
        assertTrue(blocked2.accepted());

        registry.update(TARGET, t -> t.withMaintenanceWindow(false));
        log.info("[test] target={} plan={},{}排队期间维护窗口关闭",
                TARGET, blocked.plan().planId(), blocked2.plan().planId());
        action.release(0);

        FaultInjectionPlan dead = TestAwait.awaitTerminal(
                orchestrator, blocked.plan().planId(), Duration.ofSeconds(5));
        assertEquals(PlanStatus.REJECTED, dead.status());
        assertEquals(RejectionReason.OUTSIDE_MAINTENANCE_WINDOW, dead.rejectionReason());
        FaultInjectionPlan dead2 = TestAwait.awaitTerminal(
                orchestrator, blocked2.plan().planId(), Duration.ofSeconds(5));
        assertEquals(PlanStatus.REJECTED, dead2.status());
        assertEquals(RejectionReason.OUTSIDE_MAINTENANCE_WINDOW, dead2.rejectionReason());
        // 失效计划被连续出队、队列腾空，既不挂起也不静默执行
        assertEquals(0, orchestrator.queueSize(TARGET));
        assertEquals(1, action.enteredCount(), "窗口关闭期间排队计划不得被执行");
        log.info("[test] target={} 排队计划出队重判: plan={}→{}, plan={}→{} 依据=维护窗口关闭, 队列已腾空",
                TARGET, dead.planId(), dead.rejectionReason(), dead2.planId(), dead2.rejectionReason());

        // 窗口恢复后新计划可正常执行（前面失效计划不占名额、不阻塞）
        registry.update(TARGET, t -> t.withMaintenanceWindow(true));
        SubmissionResult fresh = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "mw-fresh", true);
        assertTrue(fresh.accepted());
        action.releaseAll();
        FaultInjectionPlan terminal = TestAwait.awaitTerminal(
                orchestrator, fresh.plan().planId(), Duration.ofSeconds(10));
        assertEquals(PlanStatus.SUCCEEDED, terminal.status());
        log.info("[test] target={} 窗口恢复后plan={}终态={}", TARGET, terminal.planId(), terminal.status());
    }

    @Test
    void duplicateKeyWhileQueuedReturnsSamePlanAndExecutesOnce() throws InterruptedException {
        SubmissionResult first = orchestrator.submit(TARGET, FaultType.MEMORY_LEAK, "idem-first", true);
        awaitStatus(first.plan().planId(), PlanStatus.RUNNING, Duration.ofSeconds(5));
        SubmissionResult queued = orchestrator.submit(TARGET, FaultType.MEMORY_LEAK, "idem-key", true);
        assertTrue(queued.accepted());
        assertEquals(PlanStatus.QUEUED, orchestrator.getPlan(queued.plan().planId()).orElseThrow().status());

        // 排队阶段用同一个幂等键重复提交：判定稳定为 DUPLICATE_REQUEST，返回的是排队中的原计划
        SubmissionResult dup1 = orchestrator.submit(TARGET, FaultType.MEMORY_LEAK, "idem-key", true);
        assertEquals(RejectionReason.DUPLICATE_REQUEST, dup1.rejectionReason());
        assertEquals(queued.plan().planId(), dup1.plan().planId());
        assertEquals(PlanStatus.QUEUED, dup1.plan().status());
        // 不声明排队的重复提交同理，判定不随调用方式改变
        SubmissionResult dup2 = orchestrator.submit(TARGET, FaultType.MEMORY_LEAK, "idem-key", false);
        assertEquals(RejectionReason.DUPLICATE_REQUEST, dup2.rejectionReason());
        assertEquals(queued.plan().planId(), dup2.plan().planId());
        // 重复提交不得占队列名额：队列中仍只有原计划
        assertEquals(1, orchestrator.queueSize(TARGET));
        log.info("[test] target={} plan={}排队期间同键重复提交均判定=DUPLICATE_REQUEST, queueSize={}",
                TARGET, queued.plan().planId(), orchestrator.queueSize(TARGET));

        // 放行后原计划执行一次；终态后重放仍返回同一计划、不再执行
        action.release(0);
        action.release(1);
        FaultInjectionPlan terminal = TestAwait.awaitTerminal(
                orchestrator, queued.plan().planId(), Duration.ofSeconds(10));
        assertEquals(PlanStatus.SUCCEEDED, terminal.status());
        int executions = action.enteredCount();
        SubmissionResult replay = orchestrator.submit(TARGET, FaultType.MEMORY_LEAK, "idem-key", true);
        assertEquals(RejectionReason.DUPLICATE_REQUEST, replay.rejectionReason());
        assertEquals(queued.plan().planId(), replay.plan().planId());
        Thread.sleep(100);
        assertEquals(executions, action.enteredCount(), "同键在排队—执行—完成全过程中只能执行一次");
        log.info("[test] target={} plan={}终态后同键重放仍=DUPLICATE_REQUEST, 执行次数={}未增加",
                TARGET, queued.plan().planId(), action.enteredCount());
    }

    @Test
    void concurrentQueuedSubmissionsNeverOversellCapacityOrLosePlans() throws InterruptedException {
        // 第 1 个计划占住目标
        SubmissionResult first = orchestrator.submit(TARGET, FaultType.NETWORK_LATENCY, "cc-first", true);
        awaitStatus(first.plan().planId(), PlanStatus.RUNNING, Duration.ofSeconds(5));

        int threads = QUEUE_CAPACITY * 4; // 8 个并发排队提交，远超容量 2
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<SubmissionResult> results = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < threads; i++) {
            String key = "cc-key-" + i;
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    results.add(orchestrator.submit(TARGET, FaultType.NETWORK_LATENCY, key, true));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        assertTrue(ready.await(5, TimeUnit.SECONDS));
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        long queued = results.stream().filter(SubmissionResult::accepted).count();
        long full = results.stream()
                .filter(r -> !r.accepted() && r.rejectionReason() == RejectionReason.QUEUE_FULL).count();
        log.info("[test] target={} 并发排队提交{}个: queued={} queueFull={} queueSize={}/{}",
                TARGET, threads, queued, full, orchestrator.queueSize(TARGET), QUEUE_CAPACITY);
        assertEquals(QUEUE_CAPACITY, queued, "并发下队列容量不得超卖");
        assertEquals(threads - QUEUE_CAPACITY, full, "超额提交必须全部以 QUEUE_FULL 拒绝");
        assertEquals(QUEUE_CAPACITY, orchestrator.queueSize(TARGET));

        // 放行全部闸门：1（首计划）+ QUEUE_CAPACITY 个排队计划全部成功，无一丢失
        action.releaseAll();
        TestAwait.awaitTerminal(orchestrator, first.plan().planId(), Duration.ofSeconds(10));
        for (SubmissionResult r : results) {
            if (r.accepted()) {
                FaultInjectionPlan t = TestAwait.awaitTerminal(
                        orchestrator, r.plan().planId(), Duration.ofSeconds(10));
                assertEquals(PlanStatus.SUCCEEDED, t.status(), "排队计划不得丢失");
            }
        }
        assertEquals(1 + QUEUE_CAPACITY, action.enteredCount(), "恰好首计划+全部入队计划被执行，无重复无丢失");
        assertEquals(0, orchestrator.queueSize(TARGET));
        log.info("[test] target={} 并发排队收敛: 实际执行={} 队列清空={}",
                TARGET, action.enteredCount(), orchestrator.queueSize(TARGET));
    }
}
