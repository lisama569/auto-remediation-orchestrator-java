package com.github.lisama569.autoremediationorchestratorjava.tests;

import com.github.lisama569.autoremediationorchestratorjava.model.FaultInjectionPlan;
import com.github.lisama569.autoremediationorchestratorjava.model.FaultType;
import com.github.lisama569.autoremediationorchestratorjava.model.PlanStatus;
import com.github.lisama569.autoremediationorchestratorjava.model.RemediationPolicy;
import com.github.lisama569.autoremediationorchestratorjava.model.RejectionReason;
import com.github.lisama569.autoremediationorchestratorjava.model.SubmissionResult;
import com.github.lisama569.autoremediationorchestratorjava.model.Target;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 忙时排队与容量控制：执行顺序、容量边界、排队期间状态变化收敛、幂等键重复提交。 */
class PlanOrchestratorQueueTest {

    private static final Logger log = LoggerFactory.getLogger(PlanOrchestratorQueueTest.class);
    private static final String TARGET = "node-queue";

    private TargetRegistry registry;
    private SimulatedRemediationAction action;
    private RemediationEngine engine;

    @BeforeEach
    void setUp() {
        registry = new TargetRegistry();
        action = new SimulatedRemediationAction(registry);
        engine = new RemediationEngine(registry, action);
        registry.register(Target.healthy(TARGET, true));
    }

    private PlanOrchestrator orchestrator(RemediationPolicy policy) {
        return new PlanOrchestrator(registry, engine, policy);
    }

    @Test
    void queuedPlansExecuteFifoAfterPreviousTerminal() throws InterruptedException {
        // 动作挂起 + 200ms 超时：每个计划 RUNNING 窗口足够长，执行顺序可被稳定观测；
        // 隔离阈值调大，避免排队计划连续回滚触发隔离干扰顺序断言
        PlanOrchestrator orchestrator = orchestrator(new RemediationPolicy(0, 0, 100, 200, 8));
        action.setBehavior(TARGET, SimulatedRemediationAction.Behavior.HANG);

        SubmissionResult first = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "fifo-0");
        assertTrue(first.accepted());
        List<String> submitted = new ArrayList<>();
        submitted.add(first.plan().planId());
        for (int i = 1; i <= 4; i++) {
            SubmissionResult r = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "fifo-" + i, true);
            assertTrue(r.accepted(), "队列未满时排队提交必须被接受");
            assertEquals(PlanStatus.QUEUED, r.plan().status(), "忙时排队提交必须处于 QUEUED 状态");
            submitted.add(r.plan().planId());
        }
        // 外部查询能区分"在队列里等"与"正在执行"
        assertEquals(PlanStatus.QUEUED, orchestrator.getPlan(submitted.get(4)).orElseThrow().status());

        // 观测线程：记录各计划进入 RUNNING 的先后次序
        List<String> runningOrder = Collections.synchronizedList(new ArrayList<>());
        Set<String> seen = ConcurrentHashMap.newKeySet();
        AtomicBoolean stop = new AtomicBoolean(false);
        Thread watcher = new Thread(() -> {
            while (!stop.get()) {
                for (String id : submitted) {
                    orchestrator.getPlan(id).ifPresent(p -> {
                        if (p.status() == PlanStatus.RUNNING && seen.add(id)) {
                            runningOrder.add(id);
                        }
                    });
                }
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        });
        watcher.start();

        for (String id : submitted) {
            FaultInjectionPlan terminal = TestAwait.awaitTerminal(orchestrator, id, Duration.ofSeconds(15));
            log.info("[test] plan={} target={} 终态={} 判定依据: 前序计划到终态后按FIFO提升, 动作挂起超时回滚",
                    terminal.planId(), terminal.targetId(), terminal.status());
            assertEquals(PlanStatus.ROLLED_BACK, terminal.status());
        }
        stop.set(true);
        watcher.join(5_000);

        log.info("[test] target={} 提交顺序={} 实际执行顺序={}", TARGET, submitted, runningOrder);
        assertEquals(submitted, runningOrder, "排队计划必须按提交先后依次执行，且同一时刻只有一个在执行");
    }

    @Test
    void queueCapacityNotOversoldAndLeavesNoResidue() throws InterruptedException {
        int capacity = 3;
        // 首个计划挂起 2s 后超时回滚，留出充裕窗口完成并发提交与行为切换
        PlanOrchestrator orchestrator = orchestrator(new RemediationPolicy(0, 0, 100, 2_000, capacity));
        action.setBehavior(TARGET, SimulatedRemediationAction.Behavior.HANG);

        SubmissionResult running = orchestrator.submit(TARGET, FaultType.MEMORY_LEAK, "cap-running");
        assertTrue(running.accepted());

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<SubmissionResult> results = Collections.synchronizedList(new ArrayList<>());
        for (int i = 0; i < threads; i++) {
            String key = "cap-" + i;
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    results.add(orchestrator.submit(TARGET, FaultType.MEMORY_LEAK, key, true));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        assertTrue(ready.await(5, TimeUnit.SECONDS));
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        // 首个计划的挂起动作已经开始（不受此切换影响），后续被提升的计划将成功执行
        action.setBehavior(TARGET, SimulatedRemediationAction.Behavior.SUCCEED);

        long queued = results.stream().filter(SubmissionResult::accepted).count();
        long queueFull = results.stream()
                .filter(r -> !r.accepted() && r.rejectionReason() == RejectionReason.QUEUE_FULL).count();
        log.info("[test] target={} 并发{}个排队提交: 入队={} 队满拒绝={} 容量={}",
                TARGET, threads, queued, queueFull, capacity);
        assertEquals(capacity, queued, "并发下队列容量不得超卖");
        assertEquals(threads - capacity, queueFull, "超出容量的排队提交必须以 QUEUE_FULL 拒绝");

        // 队满拒绝不留任何残留状态：计划不可查询、幂等键未登记
        for (SubmissionResult r : results) {
            if (!r.accepted()) {
                assertEquals(RejectionReason.QUEUE_FULL, r.rejectionReason());
                assertEquals(PlanStatus.REJECTED, r.plan().status());
                assertTrue(orchestrator.getPlan(r.plan().planId()).isEmpty(),
                        "QUEUE_FULL 拒绝的计划不得在系统中留下状态");
            }
        }

        // 队列腾出位置后，被队满拒绝过的幂等键可以正常重新进入
        String reusedKey = results.stream().filter(r -> !r.accepted()).findFirst().orElseThrow()
                .plan().idempotencyKey();
        TestAwait.awaitTerminal(orchestrator, running.plan().planId(), Duration.ofSeconds(10));
        for (SubmissionResult r : results) {
            if (r.accepted()) {
                FaultInjectionPlan terminal = TestAwait.awaitTerminal(
                        orchestrator, r.plan().planId(), Duration.ofSeconds(10));
                log.info("[test] plan={} target={} 终态={} 判定依据: 队列腾出后提升执行成功",
                        terminal.planId(), terminal.targetId(), terminal.status());
                assertEquals(PlanStatus.SUCCEEDED, terminal.status(), "已入队计划不得丢失");
            }
        }
        SubmissionResult retry = orchestrator.submit(TARGET, FaultType.MEMORY_LEAK, reusedKey, true);
        log.info("[test] target={} key={} 队满拒绝后重试: accepted={}", TARGET, reusedKey, retry.accepted());
        assertTrue(retry.accepted(), "QUEUE_FULL 不登记幂等键，腾出位置后同键重试必须能正常进入");
        TestAwait.awaitTerminal(orchestrator, retry.plan().planId(), Duration.ofSeconds(10));
    }

    @Test
    void queuedPlanRejectedWhenTargetQuarantinedDuringWait() {
        PlanOrchestrator orchestrator = orchestrator(new RemediationPolicy(0, 0, 100, 300, 8));
        action.setBehavior(TARGET, SimulatedRemediationAction.Behavior.HANG);

        SubmissionResult running = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "q-running");
        assertTrue(running.accepted());
        SubmissionResult queued = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "q-waiting", true);
        assertTrue(queued.accepted());
        assertEquals(PlanStatus.QUEUED, queued.plan().status());

        // 排队期间目标被隔离
        registry.update(TARGET, Target::quarantine);
        log.info("[test] target={} 排队期间被隔离, 等待前序计划plan={}到终态", TARGET, running.plan().planId());

        TestAwait.awaitTerminal(orchestrator, running.plan().planId(), Duration.ofSeconds(10));
        FaultInjectionPlan terminal = TestAwait.awaitTerminal(
                orchestrator, queued.plan().planId(), Duration.ofSeconds(10));
        log.info("[test] plan={} target={} 终态={} 拒绝原因={} 判定依据: 提升时重新判定目标已隔离",
                terminal.planId(), terminal.targetId(), terminal.status(), terminal.rejectionReason());
        assertEquals(PlanStatus.REJECTED, terminal.status(), "排队期间目标被隔离的计划必须落到可区分终态");
        assertEquals(RejectionReason.TARGET_QUARANTINED, terminal.rejectionReason());
        // 未真正执行：目标连续失败计数只包含前序计划回滚的 1 次
        assertEquals(1, registry.get(TARGET).orElseThrow().consecutiveFailures());
    }

    @Test
    void queuedPlanRejectedWhenMaintenanceWindowClosedDuringWait() {
        PlanOrchestrator orchestrator = orchestrator(new RemediationPolicy(0, 0, 100, 300, 8));
        action.setBehavior(TARGET, SimulatedRemediationAction.Behavior.HANG);

        SubmissionResult running = orchestrator.submit(TARGET, FaultType.NETWORK_LATENCY, "w-running");
        assertTrue(running.accepted());
        SubmissionResult queued = orchestrator.submit(TARGET, FaultType.NETWORK_LATENCY, "w-waiting", true);
        assertTrue(queued.accepted());

        // 排队期间维护窗口被关闭
        registry.update(TARGET, t -> t.withMaintenanceWindow(false));
        log.info("[test] target={} 排队期间维护窗口关闭, 等待前序计划plan={}到终态", TARGET, running.plan().planId());

        TestAwait.awaitTerminal(orchestrator, running.plan().planId(), Duration.ofSeconds(10));
        FaultInjectionPlan terminal = TestAwait.awaitTerminal(
                orchestrator, queued.plan().planId(), Duration.ofSeconds(10));
        log.info("[test] plan={} target={} 终态={} 拒绝原因={} 判定依据: 提升时重新判定维护窗口已关闭",
                terminal.planId(), terminal.targetId(), terminal.status(), terminal.rejectionReason());
        assertEquals(PlanStatus.REJECTED, terminal.status());
        assertEquals(RejectionReason.OUTSIDE_MAINTENANCE_WINDOW, terminal.rejectionReason());
    }

    @Test
    void sameIdempotencyKeyResubmittedDuringQueueingYieldsStableVerdicts() {
        // 首个计划挂起 1s 后超时回滚，留出窗口完成排队中的同键重复提交
        PlanOrchestrator orchestrator = orchestrator(new RemediationPolicy(0, 0, 100, 1_000, 8));
        action.setBehavior(TARGET, SimulatedRemediationAction.Behavior.HANG);

        SubmissionResult running = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "idem-running");
        assertTrue(running.accepted());

        // 排队阶段首次提交：接受并入队
        SubmissionResult queued = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "idem-key", true);
        assertTrue(queued.accepted());
        assertEquals(PlanStatus.QUEUED, queued.plan().status());

        // 排队阶段同键重复提交：判定 DUPLICATE_REQUEST，返回排队中的原计划
        SubmissionResult dupWhileQueued = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "idem-key", true);
        assertFalse(dupWhileQueued.accepted());
        assertEquals(RejectionReason.DUPLICATE_REQUEST, dupWhileQueued.rejectionReason());
        assertEquals(queued.plan().planId(), dupWhileQueued.plan().planId());
        log.info("[test] plan={} target={} 排队中同键重复提交 判定=DUPLICATE_REQUEST status={}",
                queued.plan().planId(), TARGET, dupWhileQueued.plan().status());

        // 首个计划的挂起动作已开始（不受影响），被提升的排队计划将成功执行
        action.setBehavior(TARGET, SimulatedRemediationAction.Behavior.SUCCEED);

        // 前序计划回滚后，排队计划被提升并成功执行
        TestAwait.awaitTerminal(orchestrator, running.plan().planId(), Duration.ofSeconds(10));
        FaultInjectionPlan terminal = TestAwait.awaitTerminal(
                orchestrator, queued.plan().planId(), Duration.ofSeconds(10));
        assertEquals(PlanStatus.SUCCEEDED, terminal.status());

        // 完成之后同键再提交：仍判定 DUPLICATE_REQUEST 且返回同一计划，不造成重复执行
        SubmissionResult dupAfterDone = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "idem-key", true);
        assertFalse(dupAfterDone.accepted());
        assertEquals(RejectionReason.DUPLICATE_REQUEST, dupAfterDone.rejectionReason());
        assertEquals(queued.plan().planId(), dupAfterDone.plan().planId());
        assertEquals(PlanStatus.SUCCEEDED, dupAfterDone.plan().status());
        log.info("[test] plan={} target={} 终态={} 同键重放 判定=DUPLICATE_REQUEST, 全程只执行一次",
                terminal.planId(), TARGET, terminal.status());
    }
}
