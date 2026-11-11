package com.sbase.pipeline;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Renews active Spark pipeline leases independently of long Spark actions. */
@Component
public class PipelineLeaseHeartbeat {
    private final PipelineLeaseCoordinator coordinator;
    private final Map<UUID, ActiveLease> active = new ConcurrentHashMap<>();

    public PipelineLeaseHeartbeat(PipelineLeaseCoordinator coordinator) { this.coordinator = coordinator; }

    public void register(PipelineLeaseCoordinator.Lease lease, String workerId) {
        active.put(lease.runId(), new ActiveLease(lease, workerId));
    }

    public boolean valid(UUID runId) {
        ActiveLease lease = active.get(runId);
        return lease != null && !lease.lost;
    }

    public void unregister(UUID runId) { active.remove(runId); }

    @Scheduled(fixedDelayString = "${sbase.pipeline.lease-heartbeat-ms:15000}")
    public void renew() {
        active.forEach((id, lease) -> {
            if (!lease.lost && !coordinator.heartbeat(lease.lease, lease.workerId)) lease.lost = true;
        });
    }

    private static final class ActiveLease {
        private final PipelineLeaseCoordinator.Lease lease;
        private final String workerId;
        private volatile boolean lost;
        private ActiveLease(PipelineLeaseCoordinator.Lease lease, String workerId) {
            this.lease = lease; this.workerId = workerId;
        }
    }
}
