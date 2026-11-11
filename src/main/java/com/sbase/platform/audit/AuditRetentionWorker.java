package com.sbase.platform.audit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class AuditRetentionWorker {
    private final JdbcTemplate jdbc;
    private final int auditRetentionDays;
    public AuditRetentionWorker(JdbcTemplate jdbc,
            @Value("${sbase.audit.retention-days:180}") int auditRetentionDays) {
        this.jdbc=jdbc; this.auditRetentionDays=Math.max(30,Math.min(3650,auditRetentionDays));
    }
    @Scheduled(cron="${sbase.audit.cleanup-cron:0 20 3 * * *}", zone="UTC")
    public void purgeExpiredOperationalData() {
        jdbc.update("DELETE FROM platform_audit_events WHERE occurred_at < now() - (? * interval '1 day')",auditRetentionDays);
        jdbc.update("DELETE FROM api_rate_limit_buckets WHERE updated_at < now() - interval '2 days'");
    }
}
