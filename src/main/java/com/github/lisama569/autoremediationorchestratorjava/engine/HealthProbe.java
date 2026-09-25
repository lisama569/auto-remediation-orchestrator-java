package com.github.lisama569.autoremediationorchestratorjava.engine;

import com.github.lisama569.autoremediationorchestratorjava.domain.Target;

/** 健康校验探针。 */
@FunctionalInterface
public interface HealthProbe {
    boolean isHealthy(Target target);
}
