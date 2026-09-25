package com.github.lisama569.autoremediationorchestratorjava.engine;

import com.github.lisama569.autoremediationorchestratorjava.domain.AuditEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 判定依据审计日志。
 *
 * <p>每条事件同时写入内存（供测试断言终态与拒绝原因）和 SLF4J
 * （单测日志中可看到目标、计划与判定依据）。</p>
 */
public final class AuditLog {

    private static final Logger log = LoggerFactory.getLogger(AuditLog.class);

    private final CopyOnWriteArrayList<AuditEvent> events = new CopyOnWriteArrayList<>();

    public void record(AuditEvent event) {
        events.add(event);
        log.info("[AUDIT] t={} target={} plan={} action={} outcome={} reason={}",
                event.epochMs(),
                event.targetId() == null ? "-" : event.targetId(),
                event.planId() == null ? "-" : event.planId(),
                event.action(),
                event.outcome() == null ? "-" : event.outcome(),
                event.reason() == null ? "-" : event.reason());
    }

    public List<AuditEvent> events() {
        return List.copyOf(events);
    }
}
