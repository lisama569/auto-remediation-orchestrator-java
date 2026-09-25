package com.github.lisama569.autoremediationorchestratorjava.tests;

import com.github.lisama569.autoremediationorchestratorjava.model.FaultInjectionPlan;
import com.github.lisama569.autoremediationorchestratorjava.model.PlanStatus;
import com.github.lisama569.autoremediationorchestratorjava.service.PlanOrchestrator;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Set;

/** 测试辅助：轮询等待计划到达终态。 */
final class TestAwait {

    static final Set<PlanStatus> TERMINAL = EnumSet.of(
            PlanStatus.SUCCEEDED, PlanStatus.FAILED, PlanStatus.ROLLED_BACK, PlanStatus.REJECTED);

    private TestAwait() {}

    static FaultInjectionPlan awaitTerminal(PlanOrchestrator orchestrator, String planId, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            FaultInjectionPlan plan = orchestrator.getPlan(planId).orElseThrow();
            if (TERMINAL.contains(plan.status())) {
                return plan;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError("interrupted while awaiting terminal status");
            }
        }
        throw new AssertionError("plan " + planId + " did not reach terminal status within " + timeout);
    }
}
