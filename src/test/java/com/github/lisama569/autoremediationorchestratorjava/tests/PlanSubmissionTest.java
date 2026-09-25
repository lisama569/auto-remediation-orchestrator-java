package com.github.lisama569.autoremediationorchestratorjava.tests;

import com.github.lisama569.autoremediationorchestratorjava.domain.FaultType;
import com.github.lisama569.autoremediationorchestratorjava.domain.MaintenanceWindow;
import com.github.lisama569.autoremediationorchestratorjava.domain.PlanStatus;
import com.github.lisama569.autoremediationorchestratorjava.domain.RejectReason;
import com.github.lisama569.autoremediationorchestratorjava.domain.SubmitResult;
import com.github.lisama569.autoremediationorchestratorjava.domain.Target;
import com.github.lisama569.autoremediationorchestratorjava.domain.TargetState;
import com.github.lisama569.autoremediationorchestratorjava.service.Orchestrator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 计划下发：四类拒绝原因可区分、终态稳定；日志打印目标/计划/判定依据。
 */
class PlanSubmissionTest {

    private static final long T = 1_000L;

    private MutableClock clock;
    private Orchestrator orchestrator;
    private final AtomicInteger injectCalls = new AtomicInteger();

    @BeforeEach
    void setUp() {
        clock = new MutableClock(T);
        injectCalls.set(0);
        // 自定义故障动作：统计执行次数（默认实现仅改状态）
        Map<String, com.github.lisama569.autoremediationorchestratorjava.engine.FaultAction> actions =
                Map.of(FaultType.DISK_FULL.name(), t -> injectCalls.incrementAndGet(),
                        FaultType.NETWORK_BLACKHOLE.name(), t -> injectCalls.incrementAndGet(),
                        FaultType.CPU_BURN.name(), t -> injectCalls.incrementAndGet());
        orchestrator = new Orchestrator(Orchestrator.defaultPolicy(), clock, actions,
                Orchestrator::defaultRepair, Orchestrator.defaultHealthProbe());
        orchestrator.registerTarget("vm-1");
    }

    @AfterEach
    void tearDown() {
        orchestrator.shutdown();
    }

    private MaintenanceWindow window() {
        return new MaintenanceWindow(T - 100, T + 10_000);
    }

    private SubmitResult submit(String planId, long key) {
        SubmitResult r = orchestrator.submitPlan(planId, "vm-1", FaultType.DISK_FULL, key, window());
        System.out.println("[TEST] submit plan=" + planId + " key=" + key
                + " -> accepted=" + r.accepted()
                + " reason=" + r.rejectReason() + " msg=" + r.message());
        return r;
    }

    @Test
    @DisplayName("窗口外拒绝：OUTSIDE_MAINTENANCE_WINDOW")
    void rejectsOutsideWindow() {
        clock.advance(20_000);
        SubmitResult r = submit("p-out", 1);
        assertFalse(r.accepted());
        assertEquals(RejectReason.OUTSIDE_MAINTENANCE_WINDOW, r.rejectReason());
        assertEquals(TargetState.HEALTHY, orchestrator.getTarget("vm-1").state());
        assertEquals(0, injectCalls.get(), "拒绝的请求不得执行注入");
    }

    @Test
    @DisplayName("隔离目标拒绝：TARGET_QUARANTINED（隔离后计划与修复均被拒/停止）")
    void rejectsQuarantined() {
        // 用会持续失败的修复动作使目标连续失败 3 次后隔离
        Orchestrator o2 = new Orchestrator(Orchestrator.defaultPolicy(), clock,
                Orchestrator.defaultFaultActions(),
                t -> { throw new IllegalStateException("repair impossible"); },
                Orchestrator.defaultHealthProbe());
        o2.registerTarget("vm-q");
        try {
            long[] backoffAdvance = {0, 200, 500}; // 越过 100ms、200ms 退避点
            for (int i = 0; i < 3; i++) {
                clock.advance(backoffAdvance[i]);
                assertTrue(o2.submitPlan("qp" + i, "vm-q", FaultType.DISK_FULL,
                        200 + i, window()).accepted());
                o2.repair("vm-q");
            }
            assertEquals(TargetState.QUARANTINED, o2.getTarget("vm-q").state());

            SubmitResult r = o2.submitPlan("qp-after-quarantine", "vm-q",
                    FaultType.CPU_BURN, 299L, window());
            assertFalse(r.accepted());
            assertEquals(RejectReason.TARGET_QUARANTINED, r.rejectReason());
        } finally {
            o2.shutdown();
        }
    }

    @Test
    @DisplayName("重复幂等键拒绝：DUPLICATE_REQUEST，且不重复执行")
    void rejectsDuplicate() {
        SubmitResult first = submit("p-dup", 42);
        assertTrue(first.accepted());
        assertEquals(1, injectCalls.get());
        assertEquals(TargetState.UNHEALTHY, orchestrator.getTarget("vm-1").state());

        SubmitResult second = submit("p-dup-copy", 42);
        assertFalse(second.accepted());
        assertEquals(RejectReason.DUPLICATE_REQUEST, second.rejectReason());
        assertEquals(1, injectCalls.get(), "重复下发不得造成重复执行");
    }

    @Test
    @DisplayName("成功注入：计划 COMPLETED、目标 UNHEALTHY、无进行中计划")
    void successfulInjection() {
        SubmitResult r = submit("p-ok", 7);
        assertTrue(r.accepted());
        assertNotNull(r.plan());
        assertEquals(PlanStatus.COMPLETED, r.plan().status());

        Target t = orchestrator.getTarget("vm-1");
        assertEquals(TargetState.UNHEALTHY, t.state());
        assertEquals("DISK_FULL", t.activeFault());
        assertNull(t.activePlanId(), "执行结束后不得残留进行中计划");
    }

    @Test
    @DisplayName("注入失败：计划 FAILED、状态回滚到执行前 HEALTHY")
    void failedInjectionRollsBack() {
        Map<String, com.github.lisama569.autoremediationorchestratorjava.engine.FaultAction> failing =
                Map.of(FaultType.DISK_FULL.name(),
                        (com.github.lisama569.autoremediationorchestratorjava.engine.FaultAction)
                                t -> { throw new IllegalStateException("disk controller down"); });
        Orchestrator o2 = new Orchestrator(Orchestrator.defaultPolicy(), clock, failing,
                Orchestrator::defaultRepair, Orchestrator.defaultHealthProbe());
        o2.registerTarget("vm-x");
        try {
            SubmitResult r = o2.submitPlan("p-fail", "vm-x", FaultType.DISK_FULL, 9, window());
            assertTrue(r.accepted(), "被接受的计划即使注入失败也返回受理记录");
            assertEquals(PlanStatus.FAILED, r.plan().status());
            assertEquals(TargetState.HEALTHY, o2.getTarget("vm-x").state(),
                    "注入失败必须回滚到执行前状态");
            assertNull(o2.getTarget("vm-x").activePlanId());
            assertNull(o2.getTarget("vm-x").activeFault(), "回滚不得留下中间态");
        } finally {
            o2.shutdown();
        }
    }
}
