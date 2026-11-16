package io.sbase.server.automl;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

/** Bounded in-process asynchronous AutoML queue with cooperative Spark job cancellation. */
public final class AutoMlJobQueue implements AutoCloseable {
    private final AutoMlLifecycleService lifecycle;
    private final org.apache.spark.sql.SparkSession spark;
    private final ThreadPoolExecutor executor;
    private final ConcurrentMap<UUID, Job> jobs = new ConcurrentHashMap<>();

    public AutoMlJobQueue(AutoMlLifecycleService lifecycle,
                          org.apache.spark.sql.SparkSession spark,
                          int workers, int capacity) {
        this.lifecycle = Objects.requireNonNull(lifecycle);
        this.spark = Objects.requireNonNull(spark);
        this.executor = new ThreadPoolExecutor(Math.max(1, workers), Math.max(1, workers),
                0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(Math.max(1, capacity)),
                r -> { Thread t = new Thread(r, "sbase-automl-worker"); t.setDaemon(true); return t; },
                new ThreadPoolExecutor.AbortPolicy());
    }

    public Map<String,Object> submit(Map<String,Object> request) {
        UUID id = UUID.randomUUID();
        Job job = new Job(id);
        jobs.put(id, job);
        try {
            job.future = executor.submit(() -> {
                job.startedAt = Instant.now().toString();
                job.status = "RUNNING";
                spark.sparkContext().setJobGroup(id.toString(), "AutoML experiment " + id, true);
                try {
                    job.result = lifecycle.train(request);
                    if (!"CANCELLED".equals(job.status)) job.status = "SUCCEEDED";
                } catch (Throwable error) {
                    if (!"CANCELLED".equals(job.status)) {
                        job.status = "FAILED";
                        job.error = error.getClass().getSimpleName() + ": " + String.valueOf(error.getMessage());
                    }
                } finally {
                    spark.sparkContext().clearJobGroup();
                    job.finishedAt = Instant.now().toString();
                }
            });
        } catch (RejectedExecutionException full) {
            jobs.remove(id);
            throw new IllegalStateException("AutoML training queue is full; retry later", full);
        }
        return snapshot(job);
    }

    public Map<String,Object> status(UUID id) {
        Job job = jobs.get(id);
        if (job == null) throw new NoSuchElementException("AutoML job not found");
        return snapshot(job);
    }

    public Map<String,Object> cancel(UUID id) {
        Job job = jobs.get(id);
        if (job == null) throw new NoSuchElementException("AutoML job not found");
        if ("QUEUED".equals(job.status) || "RUNNING".equals(job.status)) {
            job.status = "CANCELLED";
            if (job.future != null) job.future.cancel(true);
            spark.sparkContext().cancelJobGroup(id.toString());
            job.finishedAt = Instant.now().toString();
        }
        return snapshot(job);
    }

    private static Map<String,Object> snapshot(Job job) {
        Map<String,Object> value = new LinkedHashMap<>();
        value.put("jobId", job.id.toString()); value.put("status", job.status);
        value.put("createdAt", job.createdAt); value.put("startedAt", job.startedAt);
        value.put("finishedAt", job.finishedAt); value.put("error", job.error);
        if (job.result != null) value.put("result", job.result);
        return value;
    }

    @Override public void close() { executor.shutdownNow(); }

    private static final class Job {
        final UUID id; final String createdAt = Instant.now().toString();
        volatile String status = "QUEUED", startedAt, finishedAt, error;
        volatile Future<?> future; volatile Map<String,Object> result;
        Job(UUID id) { this.id = id; }
    }
}
