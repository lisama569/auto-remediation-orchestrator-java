package com.github.lisama569.autoremediationorchestratorjava.model;

import java.time.Instant;
import java.util.UUID;

/** 故障注入计划。 */
public record FaultInjectionPlan(
        String planId,
        String targetId,
        FaultType faultType,
        String idempotencyKey,
        PlanStatus status,
        RejectionReason rejectionReason,
        Instant createdAt) {

    public static FaultInjectionPlan newPlan(String targetId, FaultType faultType, String idempotencyKey) {
        return new FaultInjectionPlan(UUID.randomUUID().toString(), targetId, faultType,
                idempotencyKey, PlanStatus.PENDING, null, Instant.now());
    }

    public FaultInjectionPlan withStatus(PlanStatus newStatus) {
        return new FaultInjectionPlan(planId, targetId, faultType, idempotencyKey,
                newStatus, rejectionReason, createdAt);
    }

    public FaultInjectionPlan rejected(RejectionReason reason) {
        return new FaultInjectionPlan(planId, targetId, faultType, idempotencyKey,
                PlanStatus.REJECTED, reason, createdAt);
    }
}
