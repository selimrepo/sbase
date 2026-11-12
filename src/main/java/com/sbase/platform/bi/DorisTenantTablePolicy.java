package com.sbase.platform.bi;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import java.util.Locale;
import java.util.Set;
import java.util.HashSet;

/**
 * Fail-closed registry for Doris tables that are safe for tenant-scoped access.
 * A table is usable only when it is explicitly allow-listed, registered by an
 * operator, and physically exposes the configured tenant discriminator.
 */
@Component
public class DorisTenantTablePolicy {
    private final JdbcTemplate postgres;
    private final JdbcTemplate doris;
    private final Set<String> allowList;
    private final String defaultSchema;

    public DorisTenantTablePolicy(@Qualifier("jdbcTemplate") JdbcTemplate postgres,
            @Qualifier("dorisJdbcTemplate") JdbcTemplate doris,
            @Value("${sbase.doris.allowed-tables:}") String allowedTables,
            @Value("${sbase.doris.database:analytics}") String defaultSchema) {
        this.postgres=postgres;
        this.doris=doris;
        this.defaultSchema=identifier(defaultSchema);
        this.allowList=new HashSet<>();
        for(String value:allowedTables.split(",")) if(!value.isBlank())
            this.allowList.add(value.trim().toLowerCase(Locale.ROOT));
    }

    public String requireTenantScoped(String table) {
        if(table==null || !table.matches("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?"))
            throw new IllegalArgumentException("Invalid Doris table identifier");
        String[] parts=table.split("\\.",2);
        String schema=parts.length==2?parts[0]:defaultSchema;
        String name=parts.length==2?parts[1]:parts[0];
        String canonical=schema+"."+name;
        if(!allowList.contains(canonical.toLowerCase(Locale.ROOT)) &&
                !allowList.contains(name.toLowerCase(Locale.ROOT)))
            throw new org.springframework.security.access.AccessDeniedException("Doris table is not allow-listed");
        Boolean registered=postgres.query("""
            SELECT EXISTS(SELECT 1 FROM doris_tenant_table_policies
             WHERE schema_name=? AND table_name=? AND enabled=true AND tenant_column='tenant_id' AND classification='TENANT_DATA')
            """,rs->rs.next()&&rs.getBoolean(1),schema,name);
        if(!Boolean.TRUE.equals(registered))
            throw new org.springframework.security.access.AccessDeniedException("Doris table has no active tenant isolation policy");
        Integer columns=doris.queryForObject("""
            SELECT COUNT(*) FROM information_schema.columns
             WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND LOWER(COLUMN_NAME)='tenant_id'
            """,Integer.class,schema,name);
        if(columns==null || columns!=1)
            throw new org.springframework.security.access.AccessDeniedException("Doris table does not expose exactly one tenant_id column");
        return canonical;
    }

    private static String identifier(String value) {
        if(value==null || !value.matches("[A-Za-z_][A-Za-z0-9_]*"))
            throw new IllegalArgumentException("Invalid Doris schema identifier");
        return value;
    }
}
