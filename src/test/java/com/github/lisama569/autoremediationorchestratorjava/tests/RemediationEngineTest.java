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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 修复策略、超时回滚、健康校验回滚与隔离/恢复场景。 */
class RemediationEngineTest {

    private static final Logger log = LoggerFactory.getLogger(RemediationEngineTest.class);
    private static final String TARGET = "node-remediation";
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private TargetRegistry registry;
    private SimulatedRemediationAction action;
    private PlanOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        registry = new TargetRegistry();
        action = new SimulatedRemediationAction(registry);
        RemediationEngine engine = new RemediationEngine(registry, action);
        // maxRetries=2, backoff=20ms, 隔离阈值=3, 动作超时=200ms
        orchestrator = new PlanOrchestrator(registry, engine, new RemediationPolicy(2, 20, 3, 200));
        registry.register(Target.healthy(TARGET, true));
    }

    private FaultInjectionPlan runToTerminal(String key) {
        SubmissionResult result = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, key);
        assertTrue(result.accepted(), "计划应被接受: " + key);
        return TestAwait.awaitTerminal(orchestrator, result.plan().planId(), TIMEOUT);
    }

    @Test
    void successfulRemediationResetsFailureCount() {
        action.setBehavior(TARGET, SimulatedRemediationAction.Behavior.SUCCEED);
        FaultInjectionPlan plan = runToTerminal("ok-1");
        Target target = registry.get(TARGET).orElseThrow();
        log.info("[test] plan={} target={} 终态={} 判定依据: 动作成功且健康校验通过",
                plan.planId(), TARGET, plan.status());
        assertEquals(PlanStatus.SUCCEEDED, plan.status());
        assertEquals(TargetHealth.HEALTHY, target.health());
        assertNull(target.activeFault());
        assertEquals(0, target.consecutiveFailures());
        assertFalse(target.quarantined());
    }

    @Test
    void timeoutRollsBackToPreExecutionState() {
        action.setBehavior(TARGET, SimulatedRemediationAction.Behavior.HANG);
        FaultInjectionPlan plan = runToTerminal("timeout-1");
        Target target = registry.get(TARGET).orElseThrow();
        log.info("[test] plan={} target={} 终态={} 判定依据: 动作超时回滚, health={} fault={} consecutiveFailures={}",
                plan.planId(), TARGET, plan.status(), target.health(), target.activeFault(), target.consecutiveFailures());
        assertEquals(PlanStatus.ROLLED_BACK, plan.status());
        // 回滚到执行前状态，不留中间态
        assertEquals(TargetHealth.HEALTHY, target.health());
        assertNull(target.activeFault());
        // 重试 2 次 + 首次 = 3 次尝试，但连续失败计数只累加一次（按修复流程计，不按尝试计）
        assertEquals(1, target.consecutiveFailures());
    }

    @Test
    void failedHealthCheckRollsBack() {
        action.setBehavior(TARGET, SimulatedRemediationAction.Behavior.NOOP);
        FaultInjectionPlan plan = runToTerminal("noop-1");
        Target target = registry.get(TARGET).orElseThrow();
        log.info("[test] plan={} target={} 终态={} 判定依据: 健康校验未通过回滚",
                plan.planId(), TARGET, plan.status());
        assertEquals(PlanStatus.ROLLED_BACK, plan.status());
        assertEquals(TargetHealth.HEALTHY, target.health());
        assertNull(target.activeFault());
        assertEquals(1, target.consecutiveFailures());
    }

    @Test
    void consecutiveFailuresTriggerQuarantineAndRecoveryRejoins() {
        action.setBehavior(TARGET, SimulatedRemediationAction.Behavior.FAIL);
        // 阈值=3：连续 3 次修复失败后隔离
        for (int i = 1; i <= 3; i++) {
            FaultInjectionPlan plan = runToTerminal("fail-" + i);
            assertEquals(PlanStatus.ROLLED_BACK, plan.status());
            Target t = registry.get(TARGET).orElseThrow();
            log.info("[test] plan={} target={} 第{}次失败 consecutiveFailures={} quarantined={}",
                    plan.planId(), TARGET, i, t.consecutiveFailures(), t.quarantined());
            assertEquals(i, t.consecutiveFailures(), "连续失败计数不得重复或丢失");
        }
        Target quarantined = registry.get(TARGET).orElseThrow();
        assertTrue(quarantined.quarantined(), "连续失败达阈值后必须隔离");

        // 隔离后拒绝新计划，且不再重试
        SubmissionResult rejected = orchestrator.submit(TARGET, FaultType.CPU_SPIKE, "fail-4");
        assertEquals(RejectionReason.TARGET_QUARANTINED, rejected.rejectionReason());
        assertEquals(3, registry.get(TARGET).orElseThrow().consecutiveFailures(), "隔离后不得继续计数");

        // 恢复后重新参与调度
        orchestrator.recoverTarget(TARGET);
        Target recovered = registry.get(TARGET).orElseThrow();
        assertFalse(recovered.quarantined());
        assertEquals(0, recovered.consecutiveFailures());
        action.setBehavior(TARGET, SimulatedRemediationAction.Behavior.SUCCEED);
        FaultInjectionPlan plan = runToTerminal("after-recovery");
        log.info("[test] plan={} target={} 恢复后终态={}", plan.planId(), TARGET, plan.status());
        assertEquals(PlanStatus.SUCCEEDED, plan.status());
    }

    @Test
    void rollbackDoesNotAffectOtherTargets() {
        String other = "node-other";
        registry.register(Target.healthy(other, true));
        action.setBehavior(TARGET, SimulatedRemediationAction.Behavior.FAIL);
        FaultInjectionPlan plan = runToTerminal("iso-1");
        assertEquals(PlanStatus.ROLLED_BACK, plan.status());
        Target untouched = registry.get(other).orElseThrow();
        log.info("[test] plan={} target={} 回滚后, 其他目标{}状态: health={} consecutiveFailures={}",
                plan.planId(), TARGET, other, untouched.health(), untouched.consecutiveFailures());
        assertEquals(TargetHealth.HEALTHY, untouched.health());
        assertEquals(0, untouched.consecutiveFailures());
        assertNull(untouched.activeFault());
    }
}
