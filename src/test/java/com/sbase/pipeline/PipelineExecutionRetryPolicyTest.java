package com.sbase.pipeline;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLNonTransientException;
import java.sql.SQLTransientConnectionException;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

class PipelineExecutionRetryPolicyTest {
    @Test
    void retriesTransientDatabaseAndNetworkFailures() {
        assertTrue(PipelineExecutionService.isRetryable(
            new SQLTransientConnectionException("Doris unavailable")));
        assertTrue(PipelineExecutionService.isRetryable(
            new IOException("Spark executor connection lost")));
        assertTrue(PipelineExecutionService.isRetryable(
            new TimeoutException("Doris query timeout")));
        assertTrue(PipelineExecutionService.isRetryable(
            new RuntimeException("Spark task failed", new java.sql.SQLException("connection reset", "08006"))));
    }

    @Test
    void doesNotRetryPermanentValidationAndAuthenticationFailures() {
        assertFalse(PipelineExecutionService.isRetryable(new IllegalArgumentException("Unknown column")));
        assertFalse(PipelineExecutionService.isRetryable(new SQLNonTransientException("Bad credentials", "28000")));
    }
}
