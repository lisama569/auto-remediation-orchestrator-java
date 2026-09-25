package com.github.lisama569.autoremediationorchestratorjava.web;

import com.github.lisama569.autoremediationorchestratorjava.service.Orchestrator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 进程内编排门面的 Spring 装配。 */
@Configuration
public class OrchestratorConfig {

    @Bean
    public Orchestrator orchestrator() {
        return new Orchestrator(Orchestrator.defaultPolicy());
    }
}
