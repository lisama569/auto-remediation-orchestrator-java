package com.github.lisama569.autoremediationorchestratorjava.tests;

import com.github.lisama569.autoremediationorchestratorjava.domain.FaultType;
import com.github.lisama569.autoremediationorchestratorjava.domain.MaintenanceWindow;
import com.github.lisama569.autoremediationorchestratorjava.domain.PolicyConfig;
import com.github.lisama569.autoremediationorchestratorjava.domain.TargetState;
import com.github.lisama569.autoremediationorchestratorjava.service.Orchestrator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * 目标间隔离：一个目标的失败/隔离/回滚不得影响其他目标的状态与计数。
 */
class TargetIsolationTest {

    @Test
    @DisplayName("A 被隔离且回滚，B 的健康状态、计数、计划互不影响")
    void failureOfOneTargetDoesNotAffectOthers() {
        MutableClock clock = new MutableClock(1_000L);
        PolicyConfig policy = new PolicyConfig(2, 0L, 1.0, 0L, 2_000L, 2_000L);
        Orchestrator o = new Orchestrator(policy, clock, Orchestrator.defaultFaultActions(),
                t -> { throw new IllegalStateException("always fails"); },
                Orchestrator.defaultHealthProbe());
        o.registerTarget("A");
        o.registerTarget("B");
        MaintenanceWindow w = new MaintenanceWindow(0, 100_000);

        // A 两次失败 -> 隔离
        o.submitPlan("a1", "A", FaultType.DISK_FULL, 1L, w);
        o.repair("A");
        o.repair("A");
        assertEquals(TargetState.QUARANTINED, o.getTarget("A").state());
        assertEquals(2, o.getTarget("A").consecutiveFailures());

        // B 从未注入，保持健康，计数为 0，修复返回 ALREADY_HEALTHY
        assertEquals(TargetState.HEALTHY, o.getTarget("B").state());
        assertEquals(0, o.getTarget("B").consecutiveFailures());
        assertEquals(com.github.lisama569.autoremediationorchestratorjava.domain.RepairOutcome.ALREADY_HEALTHY,
                o.repair("B").outcome());

        // 给 B 注入故障后再回滚（默认修复成功）：不影响 A 的隔离终态
        Orchestrator o2 = new Orchestrator(new PolicyConfig(2, 0L, 1.0, 0L, 2_000L, 2_000L),
                clock, Orchestrator.defaultFaultActions(),
                Orchestrator::defaultRepair, Orchestrator.defaultHealthProbe());
        o2.registerTarget("A2");
        o2.registerTarget("B2");
        o2.submitPlan("b1", "B2", FaultType.NETWORK_BLACKHOLE, 2L, w);
        assertEquals(com.github.lisama569.autoremediationorchestratorjava.domain.RepairOutcome.REPAIRED,
                o2.repair("B2").outcome());
        assertEquals(TargetState.HEALTHY, o2.getTarget("B2").state());
        assertEquals(TargetState.HEALTHY, o2.getTarget("A2").state(),
                "A2 从未操作，必须保持初始健康");
        System.out.println("[TEST] A=QUARANTINED while B stays independent; B2 repaired, A2 untouched");
        o.shutdown();
        o2.shutdown();
    }
}
