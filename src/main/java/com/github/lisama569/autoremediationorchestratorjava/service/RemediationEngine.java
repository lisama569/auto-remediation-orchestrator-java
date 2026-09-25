package com.github.lisama569.autoremediationorchestratorjava.service;

import com.github.lisama569.autoremediationorchestratorjava.model.FaultInjectionPlan;
import com.github.lisama569.autoremediationorchestratorjava.model.RemediationPolicy;
import com.github.lisama569.autoremediationorchestratorjava.model.Target;
import com.github.lisama569.autoremediationorchestratorjava.model.TargetHealth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 自动修复引擎：执行修复动作、按策略重试与线性退避、
 * 超时/健康校验失败时回滚到执行前快照、连续失败达阈值后隔离目标。
 * 每一步失败都会记录日志并体现在终态中，不被静默吞掉。
 */
@Service
public class RemediationEngine {

    private static final Logger log = LoggerFactory.getLogger(RemediationEngine.class);

    public enum RemediationOutcome {
        SUCCESS,
        ROLLED_BACK
    }

    private final TargetRegistry registry;
    private final SimulatedRemediationAction action;
    private final ExecutorService actionExecutor = Executors.newCachedThreadPool();

    public RemediationEngine(TargetRegistry registry, SimulatedRemediationAction action) {
        this.registry = registry;
        this.action = action;
    }

    /**
     * 对目标执行一次完整的"注入故障 → 修复"流程。
     * 快照在注入故障之前采集，回滚恢复到执行前状态，不留下中间态。
     * 调用方需保证同一目标同一时刻只有一个修复流程在执行。
     */
    public RemediationOutcome remediate(FaultInjectionPlan plan, RemediationPolicy policy) {
        String targetId = plan.targetId();
        Target snapshot = registry.get(targetId)
                .orElseThrow(() -> new IllegalStateException("target not registered: " + targetId));
        log.info("[remediate] plan={} target={} 执行前快照: health={} fault={} consecutiveFailures={}",
                plan.planId(), targetId, snapshot.health(), snapshot.activeFault(), snapshot.consecutiveFailures());

        registry.update(targetId, t -> t.withFault(plan.faultType().name()).withHealth(TargetHealth.FAULTY));
        log.info("[remediate] plan={} target={} 故障已注入: {}", plan.planId(), targetId, plan.faultType());

        int maxAttempts = policy.maxRetries() + 1;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            String failure = attemptOnce(targetId, plan.planId(), attempt, policy.actionTimeoutMillis());
            if (failure == null) {
                registry.update(targetId, t -> t.withFailureCount(0));
                log.info("[remediate] plan={} target={} 第{}次尝试成功, 连续失败计数已清零",
                        plan.planId(), targetId, attempt);
                return RemediationOutcome.SUCCESS;
            }
            log.warn("[remediate] plan={} target={} 第{}/{}次尝试失败, 原因: {}",
                    plan.planId(), targetId, attempt, maxAttempts, failure);
            if (attempt < maxAttempts && !backoff(plan.planId(), targetId, attempt, policy.backoffMillis())) {
                break; // 退避被中断，直接进入回滚
            }
        }
        return rollbackAndCount(plan, policy, snapshot);
    }

    /** 执行单次尝试：限时运行修复动作，随后做健康校验。返回 null 表示成功，否则返回失败原因。 */
    private String attemptOnce(String targetId, String planId, int attempt, long timeoutMillis) {
        Future<?> future = actionExecutor.submit(() -> action.execute(targetId));
        try {
            future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            return "修复动作超时(>" + timeoutMillis + "ms)，已中断";
        } catch (ExecutionException e) {
            return "修复动作异常: " + e.getCause().getMessage();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            future.cancel(true);
            return "修复动作等待被中断";
        }
        Target current = registry.get(targetId).orElseThrow();
        if (current.health() != TargetHealth.HEALTHY || current.activeFault() != null) {
            return "健康校验未通过: health=" + current.health() + " activeFault=" + current.activeFault();
        }
        return null;
    }

    /** 线性退避；返回 false 表示退避被中断。 */
    private boolean backoff(String planId, String targetId, int attempt, long backoffMillis) {
        long delay = backoffMillis * attempt;
        log.info("[backoff] plan={} target={} 第{}次尝试后退避{}ms", planId, targetId, attempt, delay);
        try {
            Thread.sleep(delay);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("[backoff] plan={} target={} 退避被中断", planId, targetId);
            return false;
        }
    }

    /**
     * 回滚到执行前快照，并原子地累加连续失败计数、按阈值判定隔离。
     * 回滚只恢复本目标的健康与故障字段，不影响隔离标志与其他目标。
     */
    private RemediationOutcome rollbackAndCount(FaultInjectionPlan plan, RemediationPolicy policy, Target snapshot) {
        String targetId = plan.targetId();
        Target updated = registry.update(targetId, current -> {
            Target rolledBack = new Target(targetId, snapshot.health(), current.quarantined(),
                    current.inMaintenanceWindow(), current.consecutiveFailures() + 1, snapshot.activeFault());
            boolean quarantine = rolledBack.consecutiveFailures() >= policy.quarantineThreshold();
            return quarantine ? rolledBack.quarantine() : rolledBack;
        });
        log.warn("[rollback] plan={} target={} 已回滚到执行前状态: health={} fault={}; 连续失败={}/{} 隔离={}",
                plan.planId(), targetId, updated.health(), updated.activeFault(),
                updated.consecutiveFailures(), policy.quarantineThreshold(), updated.quarantined());
        return RemediationOutcome.ROLLED_BACK;
    }
}
