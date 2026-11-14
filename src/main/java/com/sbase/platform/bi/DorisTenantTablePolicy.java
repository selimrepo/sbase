package com.sbase.platform.bi;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/** Fail-closed registry and physical tenant-column validation for PostgreSQL tables. */
@Component
public class DorisTenantTablePolicy {
    private final JdbcTemplate postgres;
    private final Set<String> allowList = new HashSet<>();
    private final String defaultSchema;

    public DorisTenantTablePolicy(JdbcTemplate postgres,
            @Value("${sbase.doris.allowed-tables:}") String allowedTables,
            @Value("${sbase.doris.database:public}") String defaultSchema) {
        this.postgres = postgres;
        if (defaultSchema == null || !defaultSchema.matches("[A-Za-z_][A-Za-z0-9_]*"))
            throw new IllegalArgumentException("Invalid PostgreSQL schema identifier");
        this.defaultSchema = defaultSchema;
        for (String value : allowedTables.split(","))
            if (!value.isBlank()) allowList.add(value.trim().toLowerCase(Locale.ROOT));
    }

    public String requireTenantScoped(String table) {
        if (table == null || !table.matches("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?"))
            throw new IllegalArgumentException("Invalid PostgreSQL table identifier");
        String[] parts = table.split("\\.", 2);
        String schema = parts.length == 2 ? parts[0] : defaultSchema;
        String name = parts.length == 2 ? parts[1] : parts[0];
        String canonical = schema + "." + name;
        if (!allowList.contains(canonical.toLowerCase(Locale.ROOT)) &&
                !allowList.contains(name.toLowerCase(Locale.ROOT)))
            throw new org.springframework.security.access.AccessDeniedException("PostgreSQL table is not allow-listed");

        Boolean registered = postgres.query("""
            SELECT EXISTS(SELECT 1 FROM postgres_tenant_table_policies
             WHERE schema_name=? AND table_name=? AND enabled=true
               AND tenant_column='tenant_id' AND classification='TENANT_DATA'
               AND verification_status='VERIFIED' AND verified_at IS NOT NULL
               AND invalid_tenant_rows=0)
            """, rs -> rs.next() && rs.getBoolean(1), schema, name);
        if (!Boolean.TRUE.equals(registered))
            throw new org.springframework.security.access.AccessDeniedException("PostgreSQL table has no active tenant isolation policy");

        Integer columns = postgres.queryForObject("""
            SELECT COUNT(*) FROM information_schema.columns
             WHERE table_schema=? AND table_name=? AND column_name='tenant_id'
            """, Integer.class, schema, name);
        if (columns == null || columns != 1)
            throw new org.springframework.security.access.AccessDeniedException("PostgreSQL table does not expose exactly one tenant_id column");
        return canonical;
    }
}
