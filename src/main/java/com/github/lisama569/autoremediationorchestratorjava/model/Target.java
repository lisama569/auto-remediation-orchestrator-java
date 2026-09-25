package com.github.lisama569.autoremediationorchestratorjava.model;

/** 进程内模拟的被编排目标实例状态（不可变快照）。 */
public record Target(
        String targetId,
        TargetHealth health,
        boolean quarantined,
        boolean inMaintenanceWindow,
        int consecutiveFailures,
        String activeFault) {

    public static Target healthy(String targetId, boolean inMaintenanceWindow) {
        return new Target(targetId, TargetHealth.HEALTHY, false, inMaintenanceWindow, 0, null);
    }

    public Target withHealth(TargetHealth newHealth) {
        return new Target(targetId, newHealth, quarantined, inMaintenanceWindow, consecutiveFailures, activeFault);
    }

    public Target withFault(String fault) {
        return new Target(targetId, health, quarantined, inMaintenanceWindow, consecutiveFailures, fault);
    }

    public Target withMaintenanceWindow(boolean open) {
        return new Target(targetId, health, quarantined, open, consecutiveFailures, activeFault);
    }

    public Target withFailureCount(int failures) {
        return new Target(targetId, health, quarantined, inMaintenanceWindow, failures, activeFault);
    }

    public Target quarantine() {
        return new Target(targetId, health, true, inMaintenanceWindow, consecutiveFailures, activeFault);
    }

    public Target release() {
        return new Target(targetId, health, false, inMaintenanceWindow, 0, activeFault);
    }
}
