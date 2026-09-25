package com.github.lisama569.autoremediationorchestratorjava.domain;

/**
 * 计划下发结果：成功时携带计划，拒绝时携带可区分原因与解释信息。
 */
public record SubmitResult(boolean accepted,
                           FaultInjectionPlan plan,
                           RejectReason rejectReason,
                           String message) {

    public static SubmitResult accepted(FaultInjectionPlan plan) {
        return new SubmitResult(true, plan, null,
                "plan " + plan.planId() + " accepted for target "
                        + plan.targetId());
    }

    public static SubmitResult rejected(RejectReason reason, String message) {
        if (reason == null) {
            throw new IllegalArgumentException("reject reason required");
        }
        return new SubmitResult(false, null, reason, message);
    }
}
