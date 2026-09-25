package com.github.lisama569.autoremediationorchestratorjava.domain;

/**
 * 判定依据审计事件，用于可复现、可解释。
 */
public record AuditEvent(long epochMs,
                         String targetId,
                         String planId,
                         String action,
                         String outcome,
                         String reason) {
}
