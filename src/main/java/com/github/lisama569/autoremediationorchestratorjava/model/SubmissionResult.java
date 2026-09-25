package com.github.lisama569.autoremediationorchestratorjava.model;

/** 计划下发结果：要么被接受（含计划），要么被拒绝（含可区分原因）。 */
public record SubmissionResult(boolean accepted, FaultInjectionPlan plan, RejectionReason rejectionReason) {

    public static SubmissionResult accepted(FaultInjectionPlan plan) {
        return new SubmissionResult(true, plan, null);
    }

    public static SubmissionResult rejected(FaultInjectionPlan plan, RejectionReason reason) {
        return new SubmissionResult(false, plan, reason);
    }
}
