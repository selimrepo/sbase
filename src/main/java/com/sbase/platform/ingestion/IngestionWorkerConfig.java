package com.sbase.platform.ingestion;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

/** Bounded ingestion execution prevents an unbounded backlog from exhausting heap. */
@Configuration
public class IngestionWorkerConfig {
    @Bean(name = "ingestionExecutor")
    Executor ingestionExecutor(
            @Value("${sbase.ingestion.worker.threads:4}") int threads,
            @Value("${sbase.ingestion.worker.queue-capacity:64}") int queueCapacity) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        int size = Math.max(1, Math.min(32, threads));
        executor.setCorePoolSize(size);
        executor.setMaxPoolSize(size);
        executor.setQueueCapacity(Math.max(1, Math.min(2048, queueCapacity)));
        executor.setThreadNamePrefix("sbase-ingestion-");
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(45);
        executor.setRejectedExecutionHandler(new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
        executor.initialize();
        return executor;
    }
}
