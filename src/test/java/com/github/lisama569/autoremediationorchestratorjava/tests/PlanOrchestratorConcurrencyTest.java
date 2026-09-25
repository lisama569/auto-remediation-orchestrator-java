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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 并发触发与重复下发场景：拒绝原因与终态必须稳定且可区分。 */
class PlanOrchestratorConcurrencyTest {

    private static final Logger log = LoggerFactory.getLogger(PlanOrchestratorConcurrencyTest.class);
    private static final String TARGET = "node-concurrent";

    private TargetRegistry registry;
    private SimulatedRemediationAction action;
    private PlanOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        registry = new TargetRegistry();
        action = new SimulatedRemediationAction(registry);
        RemediationEngine engine = new RemediationEngine(registry, action);
        // 较长的动作超时，保证并发提交时首个计划仍处于进行中
        orchestrator = new PlanOrchestrator(registry, engine, new RemediationPolicy(0, 10, 3, 1_000));
        registry.register(Target.healthy(TARGET, true));
    }

    @Test
    void concurrentSubmitsAllowOnlyOneActivePlan() throws InterruptedException {
        action.setBehavior(TARGET, SimulatedRemediationAction.Behavior.HANG); // 让首个计划保持进行中
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<SubmissionResult> results = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            String key = "key-" + i;
            pool.submit(() -> {
                ready.countDown();
                try {
                    go.await();
                    SubmissionResult r = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, key);
                    synchronized (results) {
                        results.add(r);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        assertTrue(ready.await(5, TimeUnit.SECONDS));
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        long accepted = results.stream().filter(SubmissionResult::accepted).count();
        long conflicts = results.stream()
                .filter(r -> !r.accepted() && r.rejectionReason() == RejectionReason.PLAN_CONFLICT).count();
        log.info("[test] target={} 并发提交{}个计划: accepted={} conflict={}", TARGET, threads, accepted, conflicts);
        assertEquals(1, accepted, "同一目标同一时刻只允许一个进行中的计划");
        assertEquals(threads - 1, conflicts, "其余请求必须以 PLAN_CONFLICT 被拒绝");

        String acceptedPlanId = results.stream().filter(SubmissionResult::accepted)
                .findFirst().orElseThrow().plan().planId();
        FaultInjectionPlan terminal = TestAwait.awaitTerminal(orchestrator, acceptedPlanId, Duration.ofSeconds(10));
        log.info("[test] plan={} target={} 终态={} 判定依据: 动作挂起超时后回滚",
                terminal.planId(), terminal.targetId(), terminal.status());
        assertEquals(PlanStatus.ROLLED_BACK, terminal.status());
    }

    @Test
    void duplicateSubmissionDoesNotCauseDuplicateExecution() throws InterruptedException {
        action.setBehavior(TARGET, SimulatedRemediationAction.Behavior.HANG);
        SubmissionResult first = orchestrator.submit(TARGET, FaultType.MEMORY_LEAK, "dup-key");
        assertTrue(first.accepted());

        // 进行中重复下发
        SubmissionResult dupWhileRunning = orchestrator.submit(TARGET, FaultType.MEMORY_LEAK, "dup-key");
        assertEquals(RejectionReason.DUPLICATE_REQUEST, dupWhileRunning.rejectionReason());
        assertEquals(first.plan().planId(), dupWhileRunning.plan().planId(), "重复下发必须返回原计划");

        FaultInjectionPlan terminal = TestAwait.awaitTerminal(
                orchestrator, first.plan().planId(), Duration.ofSeconds(10));

        // 终态后重复下发仍然不造成重复执行
        SubmissionResult dupAfterDone = orchestrator.submit(TARGET, FaultType.MEMORY_LEAK, "dup-key");
        assertEquals(RejectionReason.DUPLICATE_REQUEST, dupAfterDone.rejectionReason());
        assertEquals(first.plan().planId(), dupAfterDone.plan().planId());
        log.info("[test] plan={} target={} 终态={} 重复下发均判定=DUPLICATE_REQUEST, 未造成重复执行",
                terminal.planId(), terminal.targetId(), terminal.status());
    }

    @Test
    void concurrentDuplicateKeysYieldSingleExecution() throws InterruptedException {
        action.setBehavior(TARGET, SimulatedRemediationAction.Behavior.HANG);
        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch go = new CountDownLatch(1);
        List<SubmissionResult> results = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    go.await();
                    SubmissionResult r = orchestrator.submit(TARGET, FaultType.NETWORK_LATENCY, "same-key");
                    synchronized (results) {
                        results.add(r);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
        go.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        long accepted = results.stream().filter(SubmissionResult::accepted).count();
        long duplicates = results.stream()
                .filter(r -> !r.accepted() && r.rejectionReason() == RejectionReason.DUPLICATE_REQUEST).count();
        long conflicts = results.stream()
                .filter(r -> !r.accepted() && r.rejectionReason() == RejectionReason.PLAN_CONFLICT).count();
        log.info("[test] target={} 同键并发提交: accepted={} duplicate={} conflict={}",
                TARGET, accepted, duplicates, conflicts);
        assertEquals(1, accepted, "相同幂等键并发下发只允许一个被执行");
        // 竞争失败者按判定顺序得到 DUPLICATE_REQUEST 或 PLAN_CONFLICT，二者均为可区分拒绝
        assertEquals(threads - 1, duplicates + conflicts);
        // 关键不变量：同键不造成重复执行——已接受计划唯一
        String acceptedPlanId = results.stream().filter(SubmissionResult::accepted)
                .findFirst().orElseThrow().plan().planId();
        TestAwait.awaitTerminal(orchestrator, acceptedPlanId, Duration.ofSeconds(10));
        SubmissionResult replay = orchestrator.submit(TARGET, FaultType.NETWORK_LATENCY, "same-key");
        assertEquals(RejectionReason.DUPLICATE_REQUEST, replay.rejectionReason());
        assertEquals(acceptedPlanId, replay.plan().planId(), "同键重放必须返回原计划而非新建执行");
    }

    @Test
    void rejectionReasonsAreDistinguishable() {
        // 未处于维护窗口
        registry.register(Target.healthy("node-no-window", false));
        SubmissionResult noWindow = orchestrator.submit("node-no-window", FaultType.CPU_SPIKE, "k-window");
        assertEquals(RejectionReason.OUTSIDE_MAINTENANCE_WINDOW, noWindow.rejectionReason());

        // 目标已隔离
        registry.register(Target.healthy("node-quarantined", true).quarantine());
        SubmissionResult quarantined = orchestrator.submit("node-quarantined", FaultType.CPU_SPIKE, "k-quar");
        assertEquals(RejectionReason.TARGET_QUARANTINED, quarantined.rejectionReason());

        log.info("[test] 拒绝原因可区分: noWindow={} quarantined={}",
                noWindow.rejectionReason(), quarantined.rejectionReason());
        assertEquals(PlanStatus.REJECTED, noWindow.plan().status());
        assertEquals(PlanStatus.REJECTED, quarantined.plan().status());
    }
}
