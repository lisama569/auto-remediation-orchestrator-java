package com.github.lisama569.autoremediationorchestratorjava.tests;

import com.github.lisama569.autoremediationorchestratorjava.domain.FaultType;
import com.github.lisama569.autoremediationorchestratorjava.domain.MaintenanceWindow;
import com.github.lisama569.autoremediationorchestratorjava.domain.PolicyConfig;
import com.github.lisama569.autoremediationorchestratorjava.domain.PlanStatus;
import com.github.lisama569.autoremediationorchestratorjava.domain.RepairOutcome;
import com.github.lisama569.autoremediationorchestratorjava.domain.RepairResult;
import com.github.lisama569.autoremediationorchestratorjava.domain.SubmitResult;
import com.github.lisama569.autoremediationorchestratorjava.domain.Target;
import com.github.lisama569.autoremediationorchestratorjava.domain.TargetState;
import com.github.lisama569.autoremediationorchestratorjava.service.Orchestrator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 超时回滚专项：修复/注入动作超时必须回滚到执行前状态，不留中间态。
 * 模拟动作用可中断的 sleep 阻塞，超时后由执行器中断，测试快速、确定性收敛。
 */
class TimeoutRollbackTest {

    /** 超时 150ms，退避 0，阈值 2。 */
    private PolicyConfig policy() {
        return new PolicyConfig(2, 0L, 1.0, 0L, 150L, 150L);
    }

    private MaintenanceWindow window() {
        return new MaintenanceWindow(0, 100_000);
    }

    private void sleepUninterruptibly(long ms) {
        long deadline = System.currentTimeMillis() + ms;
        boolean interrupted = false;
        try {
            while (System.currentTimeMillis() < deadline) {
                try {
                    Thread.sleep(Math.max(1, deadline - System.currentTimeMillis()));
                } catch (InterruptedException e) {
                    interrupted = true; // 收到中断即快速结束，配合超时取消
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    @DisplayName("修复超时：TIMED_OUT，回滚到 UNHEALTHY，计数恰好 +1，无 repairRunning 残留")
    void repairTimeoutRollsBack() {
        MutableClock clock = new MutableClock(1_000L);
        Orchestrator o = new Orchestrator(policy(), clock,
                Orchestrator.defaultFaultActions(),
                t -> sleepUninterruptibly(5_000L),
                Orchestrator.defaultHealthProbe());
        o.registerTarget("tm-1");
        SubmitResult injected = o.submitPlan("p-tm1", "tm-1", FaultType.DISK_FULL,
                1L, window());
        assertTrue(injected.accepted());

        long start = System.currentTimeMillis();
        RepairResult r = o.repair("tm-1");
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("[TEST] repair timeout -> " + r.outcome()
                + " failures=" + r.consecutiveFailures() + " elapsedMs=" + elapsed);

        assertEquals(RepairOutcome.TIMED_OUT, r.outcome());
        assertEquals(1, r.consecutiveFailures(), "超时尝试必须恰好计数一次");
        assertEquals(TargetState.UNHEALTHY, r.targetState(), "必须回滚到执行前 UNHEALTHY");
        Target t = o.getTarget("tm-1");
        assertEquals("DISK_FULL", t.activeFault(), "故障标记必须恢复");
        assertNull(t.activePlanId());
        org.junit.jupiter.api.Assertions.assertFalse(t.repairRunning(),
                "不得残留 repairRunning 中间态");
        assertTrue(elapsed < 3_000, "超时判定必须在超时点附近返回，elapsed=" + elapsed);
        o.shutdown();
    }

    @Test
    @DisplayName("注入超时：计划 FAILED，目标回滚到 HEALTHY，无 activePlan/activeFault 残留")
    void injectionTimeoutRollsBack() {
        MutableClock clock = new MutableClock(1_000L);
        com.github.lisama569.autoremediationorchestratorjava.engine.FaultAction slow =
                t -> sleepUninterruptibly(5_000L);
        Map<String, com.github.lisama569.autoremediationorchestratorjava.engine.FaultAction> actions =
                Map.of(FaultType.DISK_FULL.name(), slow,
                        FaultType.NETWORK_BLACKHOLE.name(), slow,
                        FaultType.CPU_BURN.name(), slow);
        Orchestrator o = new Orchestrator(policy(), clock, actions,
                Orchestrator::defaultRepair, Orchestrator.defaultHealthProbe());
        o.registerTarget("tm-2");

        long start = System.currentTimeMillis();
        SubmitResult r = o.submitPlan("p-tm2", "tm-2", FaultType.CPU_BURN, 2L, window());
        long elapsed = System.currentTimeMillis() - start;
        System.out.println("[TEST] injection timeout -> planStatus="
                + r.plan().status() + " elapsedMs=" + elapsed);

        assertTrue(r.accepted(), "受理与注入成败解耦：超时计划仍有受理记录");
        assertEquals(PlanStatus.FAILED, r.plan().status());
        Target t = o.getTarget("tm-2");
        assertEquals(TargetState.HEALTHY, t.state(), "必须回滚到执行前 HEALTHY");
        assertNull(t.activePlanId(), "不得残留进行中计划");
        assertNull(t.activeFault(), "不得残留故障中间态");
        assertTrue(elapsed < 3_000, "超时必须快速收敛，elapsed=" + elapsed);

        // 回滚后同目标可立即下发新计划（前一计划不再占位）
        SubmitResult next = o.submitPlan("p-tm2-next", "tm-2", FaultType.DISK_FULL,
                3L, window());
        assertTrue(next.accepted());
        o.shutdown();
    }
}
