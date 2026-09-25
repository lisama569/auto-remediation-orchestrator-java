package com.github.lisama569.autoremediationorchestratorjava.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * 故障注入计划。
 *
 * <p>参数不可变；{@link PlanStatus} 在目标锁内推进，合法迁移：
 * PENDING → RUNNING → COMPLETED / FAILED。</p>
 */
public final class FaultInjectionPlan {

    private final String planId;
    private final String targetId;
    private final FaultType faultType;
    private final long idempotencyKey;
    private final MaintenanceWindow window;
    private volatile PlanStatus status;

    public FaultInjectionPlan(String planId,
                              String targetId,
                              FaultType faultType,
                              long idempotencyKey,
                              MaintenanceWindow window) {
        if (planId == null || planId.isBlank()) {
            throw new IllegalArgumentException("planId required");
        }
        if (targetId == null || targetId.isBlank()) {
            throw new IllegalArgumentException("targetId required");
        }
        if (faultType == null || window == null) {
            throw new IllegalArgumentException("faultType and window required");
        }
        this.planId = planId;
        this.targetId = targetId;
        this.faultType = faultType;
        this.idempotencyKey = idempotencyKey;
        this.window = window;
        this.status = PlanStatus.PENDING;
    }

    @JsonProperty
    public String planId() {
        return planId;
    }

    @JsonProperty
    public String targetId() {
        return targetId;
    }

    @JsonProperty
    public FaultType faultType() {
        return faultType;
    }

    @JsonProperty
    public long idempotencyKey() {
        return idempotencyKey;
    }

    @JsonProperty
    public MaintenanceWindow window() {
        return window;
    }

    @JsonProperty
    public PlanStatus status() {
        return status;
    }

    /** 持目标锁调用：PENDING → RUNNING。 */
    public void markRunning() {
        if (status != PlanStatus.PENDING) {
            throw new IllegalStateException(
                    "plan " + planId + " cannot start from " + status);
        }
        status = PlanStatus.RUNNING;
    }

    /** 持目标锁调用：RUNNING → COMPLETED。 */
    public void markCompleted() {
        if (status != PlanStatus.RUNNING) {
            throw new IllegalStateException(
                    "plan " + planId + " cannot complete from " + status);
        }
        status = PlanStatus.COMPLETED;
    }

    /** 持目标锁调用：RUNNING → FAILED（注入异常/超时且回滚完成后）。 */
    public void markFailed() {
        if (status != PlanStatus.RUNNING) {
            throw new IllegalStateException(
                    "plan " + planId + " cannot fail from " + status);
        }
        status = PlanStatus.FAILED;
    }
}
