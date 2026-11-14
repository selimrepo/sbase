package com.sbase.platform.bi;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

/** Audits physical PostgreSQL tables against the explicit tenant governance registry. */
@Service
public class PostgresTenantGovernanceAuditService {
    private static final String UUID_PATTERN =
        "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$";
    private final JdbcTemplate postgres;
    private final String schema;

    public PostgresTenantGovernanceAuditService(JdbcTemplate postgres,
            @Value("${sbase.postgres.database:public}") String schema) {
        this.postgres = postgres;
        if (schema == null || !schema.matches("[A-Za-z_][A-Za-z0-9_]*"))
            throw new IllegalArgumentException("Invalid PostgreSQL schema identifier");
        this.schema = schema;
    }

    public Map<String,Object> auditDatabase(String subject) { return auditSchema(subject, schema); }

    public Map<String,Object> auditAllDatabases(String subject) {
        requireSubject(subject);
        List<String> schemas = postgres.queryForList("""
            SELECT schema_name FROM information_schema.schemata
             WHERE schema_name NOT IN ('information_schema','pg_catalog')
               AND schema_name NOT LIKE 'pg_toast%'
             ORDER BY schema_name
            """, String.class);
        List<Map<String,Object>> inventories = new ArrayList<>();
        List<Map<String,Object>> tables = new ArrayList<>();
        int total=0, verified=0, blocked=0, missing=0, unregistered=0;
        boolean complete=true;
        for (String item : schemas) {
            Map<String,Object> inventory = auditSchema(subject, item);
            inventories.add(inventory);
            total += number(inventory.get("tableCount"));
            verified += number(inventory.get("verifiedShapeCount"));
            blocked += number(inventory.get("blockedCount"));
            missing += number(inventory.get("missingTenantColumnCount"));
            unregistered += number(inventory.get("unregisteredCount"));
            complete &= Boolean.TRUE.equals(inventory.get("complete"));
            @SuppressWarnings("unchecked")
            List<Map<String,Object>> rows = (List<Map<String,Object>>) inventory.get("tables");
            tables.addAll(rows);
        }
        Map<String,Object> result = new LinkedHashMap<>();
        result.put("databaseCount", inventories.size()); result.put("tableCount", total);
        result.put("verifiedShapeCount", verified); result.put("blockedCount", blocked);
        result.put("missingTenantColumnCount", missing); result.put("unregisteredCount", unregistered);
        result.put("complete", complete); result.put("databases", inventories); result.put("tables", tables);
        return result;
    }

    private Map<String,Object> auditSchema(String subject, String schemaName) {
        requireSubject(subject);
        if (!schemaName.matches("[A-Za-z_][A-Za-z0-9_]*"))
            throw new IllegalArgumentException("Invalid PostgreSQL schema identifier");
        List<Map<String,Object>> physical = postgres.queryForList("""
            SELECT table_name, table_type FROM information_schema.tables
             WHERE table_schema=? ORDER BY table_name
            """, schemaName);
        List<Map<String,Object>> results = new ArrayList<>();
        int verified=0, blocked=0, missing=0, unregistered=0;
        for (Map<String,Object> item : physical) {
            String table = Objects.toString(item.get("table_name"), "");
            if (!table.matches("[A-Za-z_][A-Za-z0-9_]*"))
                throw new IllegalStateException("PostgreSQL returned an unsafe table identifier");
            Integer columns = postgres.queryForObject("""
                SELECT COUNT(*) FROM information_schema.columns
                 WHERE table_schema=? AND table_name=? AND column_name='tenant_id'
                """, Integer.class, schemaName, table);
            boolean hasTenantColumn = columns != null && columns == 1;
            Boolean registered = postgres.query("""
                SELECT EXISTS(SELECT 1 FROM postgres_tenant_table_policies
                 WHERE schema_name=? AND table_name=? AND enabled=true
                   AND tenant_column='tenant_id' AND classification='TENANT_DATA'
                   AND verification_status='VERIFIED' AND verified_at IS NOT NULL
                   AND invalid_tenant_rows=0 AND verified_row_count IS NOT NULL)
                """, rs -> rs.next() && rs.getBoolean(1), schemaName, table);
            long rowCount=0, invalidRows=-1;
            if (hasTenantColumn) {
                String qualified = "\"" + schemaName + "\".\"" + table + "\"";
                Map<String,Object> counts = postgres.queryForMap(
                    "SELECT COUNT(*) AS row_count, COALESCE(SUM(CASE WHEN tenant_id IS NULL OR tenant_id::text !~ ? THEN 1 ELSE 0 END),0) AS invalid_rows FROM " + qualified,
                    UUID_PATTERN);
                rowCount = number(counts.get("row_count"));
                invalidRows = number(counts.get("invalid_rows"));
            }
            Boolean rowCountMatches = false;
            if (hasTenantColumn && Boolean.TRUE.equals(registered)) {
                rowCountMatches = postgres.query("""
                    SELECT EXISTS(SELECT 1 FROM postgres_tenant_table_policies
                     WHERE schema_name=? AND table_name=? AND verified_row_count=? AND invalid_tenant_rows=0)
                    """, rs -> rs.next() && rs.getBoolean(1), schemaName, table, rowCount);
            }
            String status;
            if (!hasTenantColumn) { status="MISSING_TENANT_COLUMN"; missing++; blocked++; }
            else if (!Boolean.TRUE.equals(registered)) { status="UNREGISTERED_OR_UNVERIFIED"; unregistered++; blocked++; }
            else if (invalidRows != 0) { status="INVALID_TENANT_ROWS"; blocked++; }
            else if (!rowCountMatches) { status="STALE_VERIFICATION"; blocked++; }
            else { status="VERIFIED_SHAPE"; verified++; }

            if (!"VERIFIED_SHAPE".equals(status) && Boolean.TRUE.equals(registered)) {
                String rejected = "INVALID_TENANT_ROWS".equals(status) || "MISSING_TENANT_COLUMN".equals(status) ? "REJECTED" : "PENDING";
                if (invalidRows >= 0) postgres.update("""
                    UPDATE postgres_tenant_table_policies SET enabled=FALSE,verification_status=?,
                           invalid_tenant_rows=?,verified_at=NULL,updated_at=now()
                     WHERE schema_name=? AND table_name=?
                    """, rejected, invalidRows, schemaName, table);
                else postgres.update("""
                    UPDATE postgres_tenant_table_policies SET enabled=FALSE,verification_status=?,
                           verified_at=NULL,updated_at=now()
                     WHERE schema_name=? AND table_name=?
                    """, rejected, schemaName, table);
            }
            Map<String,Object> row = new LinkedHashMap<>();
            row.put("schema",schemaName); row.put("table",table);
            row.put("objectType",Objects.toString(item.get("table_type"),"UNKNOWN"));
            row.put("status",status); row.put("rowCount",rowCount);
            row.put("invalidTenantRows",invalidRows); row.put("registered",Boolean.TRUE.equals(registered));
            results.add(row);
            postgres.update("""
                INSERT INTO postgres_tenant_policy_audit(schema_name,table_name,subject,action,outcome,details)
                VALUES(?,?,?,'INVENTORY_SCAN',?,?::jsonb)
                """, schemaName, table, subject, status,
                "{\"rowCount\":"+rowCount+",\"invalidTenantRows\":"+invalidRows+
                    ",\"hasTenantColumn\":"+hasTenantColumn+",\"registered\":"+Boolean.TRUE.equals(registered)+"}");
        }
        Map<String,Object> summary = new LinkedHashMap<>();
        summary.put("schema",schemaName); summary.put("tableCount",results.size());
        summary.put("verifiedShapeCount",verified); summary.put("blockedCount",blocked);
        summary.put("missingTenantColumnCount",missing); summary.put("unregisteredCount",unregistered);
        summary.put("tables",results); summary.put("complete",blocked==0);
        return summary;
    }

    private static void requireSubject(String subject) {
        if (subject == null || subject.isBlank()) throw new IllegalArgumentException("Audit subject is required");
    }
    private static long number(Object value) { return value instanceof Number n ? n.longValue() : 0L; }
}
