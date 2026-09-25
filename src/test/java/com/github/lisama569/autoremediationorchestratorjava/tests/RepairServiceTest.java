package com.github.lisama569.autoremediationorchestratorjava.tests;

import com.github.lisama569.autoremediationorchestratorjava.domain.FaultType;
import com.github.lisama569.autoremediationorchestratorjava.domain.MaintenanceWindow;
import com.github.lisama569.autoremediationorchestratorjava.domain.PolicyConfig;
import com.github.lisama569.autoremediationorchestratorjava.domain.RepairOutcome;
import com.github.lisama569.autoremediationorchestratorjava.domain.RepairResult;
import com.github.lisama569.autoremediationorchestratorjava.domain.SubmitResult;
import com.github.lisama569.autoremediationorchestratorjava.domain.Target;
import com.github.lisama569.autoremediationorchestratorjava.domain.TargetState;
import com.github.lisama569.autoremediationorchestratorjava.service.Orchestrator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 修复策略：失败计数、指数退避、阈值隔离、恢复后重新调度、健康校验失败回滚。
 */
class RepairServiceTest {

    private MutableClock clock;

    /** 阈值 2、退避 100ms*2^n（上限 10s）、动作超时 2s。 */
    private PolicyConfig policy() {
        return new PolicyConfig(2, 100L, 2.0, 10_000L, 2_000L, 2_000L);
    }

    private Orchestrator build(
            com.github.lisama569.autoremediationorchestratorjava.engine.RepairAction repair,
            com.github.lisama569.autoremediationorchestratorjava.engine.HealthProbe probe) {
        clock = new MutableClock(1_000L);
        return new Orchestrator(policy(), clock, Orchestrator.defaultFaultActions(),
                repair, probe);
    }

    private void inject(Orchestrator o, String target, String plan, long key) {
        SubmitResult r = o.submitPlan(plan, target, FaultType.DISK_FULL, key,
                new MaintenanceWindow(0, 100_000));
        assertTrue(r.accepted());
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    @DisplayName("健康目标无需修复：ALREADY_HEALTHY，不计数")
    void healthyTargetSkips() {
        Orchestrator o = build(Orchestrator::defaultRepair, Orchestrator.defaultHealthProbe());
        o.registerTarget("t1");
        RepairResult r = o.repair("t1");
        System.out.println("[TEST] repair healthy -> " + r.outcome()
                + " failures=" + r.consecutiveFailures());
        assertEquals(RepairOutcome.ALREADY_HEALTHY, r.outcome());
        assertEquals(0, r.consecutiveFailures());
        o.shutdown();
    }

    @Test
    @DisplayName("修复成功：REPAIRED，目标 HEALTHY、计数归零")
    void repairSuccess() {
        Orchestrator o = build(Orchestrator::defaultRepair, Orchestrator.defaultHealthProbe());
        o.registerTarget("t2");
        inject(o, "t2", "p2", 1L);
        RepairResult r = o.repair("t2");
        System.out.println("[TEST] repair -> " + r.outcome() + " state=" + r.targetState());
        assertEquals(RepairOutcome.REPAIRED, r.outcome());
        assertEquals(TargetState.HEALTHY, o.getTarget("t2").state());
        o.shutdown();
    }

    @Test
    @DisplayName("失败计数 + 指数退避：窗口内 BACKOFF_SKIPPED 不计数；越过后第二次失败触发隔离")
    void failureBackoffAndQuarantine() {
        Orchestrator o = build(
                t -> { throw new IllegalStateException("disk busy"); },
                Orchestrator.defaultHealthProbe());
        o.registerTarget("t3");
        inject(o, "t3", "p3", 1L);

        RepairResult first = o.repair("t3");
        assertEquals(RepairOutcome.FAILED, first.outcome());
        assertEquals(1, first.consecutiveFailures());
        assertEquals(1_100L, first.nextEligibleAtMs(), "首次退避 100ms");
        assertEquals(TargetState.UNHEALTHY, first.targetState(),
                "失败后回滚到执行前 UNHEALTHY");

        RepairResult skipped = o.repair("t3");
        assertEquals(RepairOutcome.BACKOFF_SKIPPED, skipped.outcome(),
                "退避窗口内跳过");
        assertEquals(1, skipped.consecutiveFailures(), "跳过不得计数");
        System.out.println("[TEST] during backoff -> " + skipped.outcome()
                + " failures=" + skipped.consecutiveFailures());

        clock.advance(101);
        RepairResult second = o.repair("t3");
        System.out.println("[TEST] second failure -> " + second.outcome()
                + " failures=" + second.consecutiveFailures() + " state=" + second.targetState());
        assertEquals(RepairOutcome.QUARANTINED, second.outcome());
        assertEquals(TargetState.QUARANTINED, o.getTarget("t3").state());

        // 隔离后停止重试
        RepairResult after = o.repair("t3");
        assertEquals(RepairOutcome.ALREADY_QUARANTINED, after.outcome());
        assertEquals(2, after.consecutiveFailures(), "隔离后重试不增加计数");
        o.shutdown();
    }

    @Test
    @DisplayName("隔离后恢复：重新参与调度，注入与修复恢复正常")
    void recoverRejoinsScheduling() {
        Orchestrator o = build(
                t -> { throw new IllegalStateException("disk busy"); },
                Orchestrator.defaultHealthProbe());
        o.registerTarget("t4");
        inject(o, "t4", "p4a", 1L);
        o.repair("t4");
        clock.advance(200);
        o.repair("t4");
        assertEquals(TargetState.QUARANTINED, o.getTarget("t4").state());

        // 隔离期间计划仍被拒
        SubmitResult blocked = o.submitPlan("p4b", "t4", FaultType.CPU_BURN, 2L,
                new MaintenanceWindow(0, 100_000));
        assertFalse(blocked.accepted());

        boolean recovered = o.recover("t4");
        assertTrue(recovered);
        assertEquals(TargetState.HEALTHY, o.getTarget("t4").state());
        assertEquals(0, o.getTarget("t4").consecutiveFailures(), "恢复后计数清零");
        System.out.println("[TEST] recovered, failures reset to 0");

        // 恢复后可再次注入
        SubmitResult again = o.submitPlan("p4c", "t4", FaultType.NETWORK_BLACKHOLE, 3L,
                new MaintenanceWindow(0, 100_000));
        assertTrue(again.accepted());
        assertEquals(TargetState.UNHEALTHY, o.getTarget("t4").state());
        o.shutdown();
    }

    @Test
    @DisplayName("健康校验未通过：HEALTH_CHECK_FAILED，回滚且计数一次")
    void healthCheckFailureRollsBack() {
        // 修复动作把目标改健康，但探针始终返回 false
        Orchestrator o = build(Orchestrator::defaultRepair, t -> false);
        o.registerTarget("t5");
        inject(o, "t5", "p5", 1L);

        RepairResult r = o.repair("t5");
        System.out.println("[TEST] probe-false -> " + r.outcome()
                + " failures=" + r.consecutiveFailures() + " state=" + r.targetState());
        assertEquals(RepairOutcome.HEALTH_CHECK_FAILED, r.outcome());
        assertEquals(1, r.consecutiveFailures());
        assertEquals(TargetState.UNHEALTHY, r.targetState(),
                "探针未通过必须回滚到执行前 UNHEALTHY");
        Target t = o.getTarget("t5");
        assertEquals("DISK_FULL", t.activeFault(), "回滚必须恢复故障标记，不留中间态");
        o.shutdown();
    }

    @Test
    @DisplayName("失败后成功：计数归零，退避清除")
    void failureThenSuccessResets() {
        boolean[] failOnce = {true};
        Orchestrator o = build(
                t -> {
                    if (failOnce[0]) {
                        failOnce[0] = false;
                        throw new IllegalStateException("transient");
                    }
                    Orchestrator.defaultRepair(t);
                },
                Orchestrator.defaultHealthProbe());
        o.registerTarget("t6");
        inject(o, "t6", "p6", 1L);

        RepairResult f = o.repair("t6");
        assertEquals(RepairOutcome.FAILED, f.outcome());
        clock.advance(500);
        RepairResult s = o.repair("t6");
        assertEquals(RepairOutcome.REPAIRED, s.outcome());
        assertEquals(0, o.getTarget("t6").consecutiveFailures());
        assertEquals(0L, o.getTarget("t6").nextEligibleAtMs());
        System.out.println("[TEST] recovered after transient failure, counters reset");
        o.shutdown();
    }
}
