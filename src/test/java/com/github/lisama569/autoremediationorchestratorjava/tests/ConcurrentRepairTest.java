package com.github.lisama569.autoremediationorchestratorjava.tests;

import com.github.lisama569.autoremediationorchestratorjava.domain.FaultType;
import com.github.lisama569.autoremediationorchestratorjava.domain.MaintenanceWindow;
import com.github.lisama569.autoremediationorchestratorjava.domain.PolicyConfig;
import com.github.lisama569.autoremediationorchestratorjava.domain.RepairOutcome;
import com.github.lisama569.autoremediationorchestratorjava.domain.RepairResult;
import com.github.lisama569.autoremediationorchestratorjava.domain.TargetState;
import com.github.lisama569.autoremediationorchestratorjava.service.Orchestrator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
 * 并发修复触发：计数不重不漏。
 *
 * <p>胜出修复动作在独立线程中阻塞（持占位 repairRunning），期间其余并发触发
 * 全部得到 ALREADY_RUNNING 且不计数；最终动作失败恰好计数一次。</p>
 */
class ConcurrentRepairTest {

    private static final int RIVALS = 11;

    @Test
    @DisplayName("修复进行中并发触发：1 次执行(+1) + 11 个 ALREADY_RUNNING(不计数)")
    void concurrentRepairNoDoubleCounting() throws Exception {
        MutableClock clock = new MutableClock(1_000L);
        PolicyConfig policy = new PolicyConfig(99, 0L, 1.0, 0L, 30_000L, 30_000L);

        AtomicInteger actionInvocations = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        com.github.lisama569.autoremediationorchestratorjava.engine.RepairAction blockingFail = t -> {
            actionInvocations.incrementAndGet();
            entered.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("repair boom");
        };

        Orchestrator o = new Orchestrator(policy, clock, Orchestrator.defaultFaultActions(),
                blockingFail, Orchestrator.defaultHealthProbe());
        o.registerTarget("cr-1");
        assertTrue(o.submitPlan("p-cr", "cr-1", FaultType.DISK_FULL, 1L,
                new MaintenanceWindow(0, 100_000)).accepted());

        ExecutorService pool = Executors.newCachedThreadPool();
        try {
            // 胜出者开始修复并进入阻塞动作
            Future<RepairResult> winner = pool.submit(() -> o.repair("cr-1"));
            assertTrue(entered.await(5, TimeUnit.SECONDS), "winner must enter repair action");

            // 修复进行中：其余并发触发必须立即得到 ALREADY_RUNNING
            CountDownLatch start = new CountDownLatch(1);
            Map<Integer, RepairResult> rivalResults = new ConcurrentHashMap<>();
            for (int i = 0; i < RIVALS; i++) {
                final int idx = i;
                pool.submit(() -> {
                    start.await();
                    rivalResults.put(idx, o.repair("cr-1"));
                    return null;
                });
            }
            start.countDown();

            long deadline = System.currentTimeMillis() + 5_000;
            while (rivalResults.size() < RIVALS && System.currentTimeMillis() < deadline) {
                Thread.sleep(5);
            }
            assertEquals(RIVALS, rivalResults.size(), "所有竞争触发必须在胜出者执行期间完成判定");
            for (var e : rivalResults.entrySet()) {
                assertEquals(RepairOutcome.ALREADY_RUNNING, e.getValue().outcome(),
                        "rival " + e.getKey());
                assertEquals(0, e.getValue().consecutiveFailures(),
                        "ALREADY_RUNNING 不得增加计数，rival " + e.getKey());
            }
            assertEquals(0, o.getTarget("cr-1").consecutiveFailures(),
                    "动作执行期间失败尚未计数");

            // 放行胜出者，动作失败 -> 回滚 -> 恰好计数一次
            release.countDown();
            RepairResult winnerResult = winner.get(5, TimeUnit.SECONDS);
            System.out.println("[TEST] winner=" + winnerResult.outcome()
                    + " failures=" + winnerResult.consecutiveFailures()
                    + " actionInvocations=" + actionInvocations.get());
            assertEquals(RepairOutcome.FAILED, winnerResult.outcome());
            assertEquals(1, actionInvocations.get(), "修复动作只执行一次");
            assertEquals(1, o.getTarget("cr-1").consecutiveFailures(),
                    "并发下失败计数恰好为 1，不重复、不丢失");
            assertEquals(TargetState.UNHEALTHY, o.getTarget("cr-1").state());
            assertFalse(o.getTarget("cr-1").repairRunning(), "不得残留 repairRunning");
        } finally {
            release.countDown();
            pool.shutdownNow();
            pool.awaitTermination(5, TimeUnit.SECONDS);
            o.shutdown();
        }
    }
}
