package com.github.lisama569.autoremediationorchestratorjava.tests;

import com.github.lisama569.autoremediationorchestratorjava.domain.FaultType;
import com.github.lisama569.autoremediationorchestratorjava.domain.MaintenanceWindow;
import com.github.lisama569.autoremediationorchestratorjava.domain.PolicyConfig;
import com.github.lisama569.autoremediationorchestratorjava.domain.RejectReason;
import com.github.lisama569.autoremediationorchestratorjava.domain.SubmitResult;
import com.github.lisama569.autoremediationorchestratorjava.domain.TargetState;
import com.github.lisama569.autoremediationorchestratorjava.service.Orchestrator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 并发触发：同一目标在计划进行中时，其他下发请求必须被稳定拒绝为
 * PLAN_ALREADY_IN_PROGRESS；同一时刻绝不会有两个计划执行。
 */
class ConcurrentPlanTest {

    private MutableClock clock;
    private Orchestrator orchestrator;
    private final AtomicInteger applied = new AtomicInteger();
    private final CountDownLatch actionEntered = new CountDownLatch(1);
    private final CountDownLatch releaseAction = new CountDownLatch(1);
    private ExecutorService pool;

    @BeforeEach
    void setUp() {
        clock = new MutableClock(1_000L);
        applied.set(0);
        com.github.lisama569.autoremediationorchestratorjava.engine.FaultAction blocking = t -> {
            applied.incrementAndGet();
            actionEntered.countDown();
            try {
                releaseAction.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("blocking action interrupted", e);
            }
        };
        com.github.lisama569.autoremediationorchestratorjava.engine.FaultAction noop = t -> { };
        Map<String, com.github.lisama569.autoremediationorchestratorjava.engine.FaultAction> actions =
                Map.of(FaultType.DISK_FULL.name(), blocking,
                        FaultType.NETWORK_BLACKHOLE.name(), noop,
                        FaultType.CPU_BURN.name(), noop);
        PolicyConfig policy = new PolicyConfig(3, 0L, 1.0, 0L, 30_000L, 30_000L);
        orchestrator = new Orchestrator(policy, clock, actions,
                Orchestrator::defaultRepair, Orchestrator.defaultHealthProbe());
        orchestrator.registerTarget("vm-c");
        pool = Executors.newCachedThreadPool();
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        releaseAction.countDown();
        pool.shutdownNow();
        pool.awaitTermination(5, TimeUnit.SECONDS);
        orchestrator.shutdown();
    }

    private MaintenanceWindow window() {
        return new MaintenanceWindow(0, 100_000);
    }

    @Test
    @DisplayName("进行中期间：其余请求全部 PLAN_ALREADY_IN_PROGRESS；进行中计数恒为 1")
    void conflictWhilePlanInProgress() throws Exception {
        // 胜出者在独立线程开始下发（其注入动作会阻塞，期间持有目标锁）
        Future<SubmitResult> winnerFuture = pool.submit(() ->
                orchestrator.submitPlan("plan-win", "vm-c", FaultType.DISK_FULL,
                        1_000L, window()));
        assertTrue(actionEntered.await(5, TimeUnit.SECONDS), "winning plan must enter action");

        // 胜出计划进行中：并发发起多个其他请求，必须全部被拒绝，且原因可区分、稳定
        int rivals = 6;
        CountDownLatch start = new CountDownLatch(1);
        Map<Integer, SubmitResult> rivalResults = new java.util.concurrent.ConcurrentHashMap<>();
        for (int i = 0; i < rivals; i++) {
            final int idx = i;
            pool.submit(() -> {
                start.await();
                rivalResults.put(idx, orchestrator.submitPlan("plan-rival-" + idx,
                        "vm-c", FaultType.NETWORK_BLACKHOLE, 2_000L + idx, window()));
                return null;
            });
        }
        start.countDown();

        long deadline = System.currentTimeMillis() + 5_000;
        while (rivalResults.size() < rivals && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(rivals, rivalResults.size(), "所有竞争请求必须在胜出者执行期间完成判定");
        for (var e : rivalResults.entrySet()) {
            SubmitResult r = e.getValue();
            System.out.println("[TEST] rival " + e.getKey()
                    + " accepted=" + r.accepted() + " reason=" + r.rejectReason());
            assertFalse(r.accepted());
            assertEquals(RejectReason.PLAN_ALREADY_IN_PROGRESS, r.rejectReason());
        }
        assertEquals(1, applied.get(), "进行中只允许一个注入动作执行");

        // 放行胜出者，等待其完成
        releaseAction.countDown();
        SubmitResult winner = winnerFuture.get(5, TimeUnit.SECONDS);
        assertTrue(winner.accepted());
        assertEquals(TargetState.UNHEALTHY, orchestrator.getTarget("vm-c").state());
        assertNullActivePlan();
        assertEquals(1, applied.get(), "全过程注入动作恰好执行一次");
        System.out.println("[TEST] winner completed, targetState="
                + orchestrator.getTarget("vm-c").state());
    }

    @Test
    @DisplayName("胜出者结束后排队请求串行受理：任意时刻 activePlan 唯一，终态 UNHEALTHY")
    void queuedRequestsSerializeAfterCompletion() throws Exception {
        int n = 4;
        CountDownLatch start = new CountDownLatch(1);
        Map<Integer, SubmitResult> results = new java.util.concurrent.ConcurrentHashMap<>();
        // 4 个线程同时下发同一目标；动作串行，瞬时进行中计划恒为一个（由锁保证），
        // 每个请求要么在他人进行中时被拒，要么在他人结束后受理——不会并发执行。
        for (int i = 0; i < n; i++) {
            final int idx = i;
            pool.submit(() -> {
                start.await();
                results.put(idx, orchestrator.submitPlan("plan-q-" + idx, "vm-c",
                        FaultType.DISK_FULL, 3_000L + idx, window()));
                return null;
            });
        }
        start.countDown();
        assertTrue(actionEntered.await(5, TimeUnit.SECONDS));
        // 第一个进行中时立刻再发一个，必然被拒
        SubmitResult extra = orchestrator.submitPlan("plan-extra", "vm-c",
                FaultType.CPU_BURN, 9_999L, window());
        assertFalse(extra.accepted());
        assertEquals(RejectReason.PLAN_ALREADY_IN_PROGRESS, extra.rejectReason());

        releaseAction.countDown();
        deadline2(results, n);

        long accepted = results.values().stream().filter(SubmitResult::accepted).count();
        long conflict = results.values().stream().filter(
                r -> !r.accepted() && r.rejectReason() == RejectReason.PLAN_ALREADY_IN_PROGRESS)
                .count();
        System.out.println("[TEST] queued: accepted=" + accepted + " conflict=" + conflict
                + " applied=" + applied.get());
        assertEquals(n, accepted + conflict, "每个请求终态可区分且无丢失");
        assertEquals(accepted, applied.get(), "被受理的计划数 == 实际执行动作数");
        assertNullActivePlan();
    }

    private void deadline2(Map<Integer, SubmitResult> results, int n) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (results.size() < n && System.currentTimeMillis() < deadline) {
            Thread.sleep(5);
        }
        assertEquals(n, results.size());
    }

    private void assertNullActivePlan() {
        assertEquals(null, orchestrator.getTarget("vm-c").activePlanId(),
                "执行结束后不得残留进行中计划");
    }
}
