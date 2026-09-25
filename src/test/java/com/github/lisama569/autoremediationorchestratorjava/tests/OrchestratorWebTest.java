package com.github.lisama569.autoremediationorchestratorjava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Web 冒烟：拒绝返回 409 且 body 中 rejectReason 可区分；审计端点可查询判定依据。
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrchestratorWebTest {

    @Autowired
    private TestRestTemplate rest;

    private Map<String, Object> planBody(String planId, String targetId,
                                         String faultType, long key, long start, long end) {
        return Map.of("planId", planId, "targetId", targetId, "faultType", faultType,
                "idempotencyKey", key, "windowStartMs", start, "windowEndMs", end);
    }

    @Test
    @SuppressWarnings("unchecked")
    void rejectionReasonsAreDistinguishableOverHttp() {
        rest.postForEntity("/api/targets/web-1", null, Map.class);

        long now = System.currentTimeMillis();

        // 窗口外 -> 409 OUTSIDE_MAINTENANCE_WINDOW
        ResponseEntity<Map> out = rest.postForEntity("/api/plans",
                new HttpEntity<>(planBody("w-out", "web-1", "DISK_FULL", 1001L,
                        now - 10_000, now - 1_000)), Map.class);
        assertEquals(HttpStatus.CONFLICT, out.getStatusCode());
        assertEquals("OUTSIDE_MAINTENANCE_WINDOW", out.getBody().get("rejectReason"));
        System.out.println("[TEST][HTTP] outside-window -> " + out.getBody().get("rejectReason"));

        // 窗口内 -> 200
        ResponseEntity<Map> ok = rest.postForEntity("/api/plans",
                new HttpEntity<>(planBody("w-ok", "web-1", "DISK_FULL", 1002L,
                        now - 1_000, now + 60_000)), Map.class);
        assertEquals(HttpStatus.OK, ok.getStatusCode());
        assertEquals(Boolean.TRUE, ok.getBody().get("accepted"));

        // 重复键 -> 409 DUPLICATE_REQUEST
        ResponseEntity<Map> dup = rest.postForEntity("/api/plans",
                new HttpEntity<>(planBody("w-dup", "web-1", "DISK_FULL", 1002L,
                        now - 1_000, now + 60_000)), Map.class);
        assertEquals(HttpStatus.CONFLICT, dup.getStatusCode());
        assertEquals("DUPLICATE_REQUEST", dup.getBody().get("rejectReason"));
        System.out.println("[TEST][HTTP] duplicate -> " + dup.getBody().get("rejectReason"));

        // 修复 -> REPAIRED（默认修复动作成功）
        ResponseEntity<Map> repaired = rest.postForEntity(
                "/api/targets/web-1/repair", null, Map.class);
        assertEquals("REPAIRED", repaired.getBody().get("outcome"));

        // 审计端点包含可解释的判定依据
        ResponseEntity<java.util.List> audit =
                rest.getForEntity("/api/audit", java.util.List.class);
        assertEquals(HttpStatus.OK, audit.getStatusCode());
        assertTrue(audit.getBody().size() >= 3);
        System.out.println("[TEST][HTTP] audit events=" + audit.getBody().size());
    }
}
