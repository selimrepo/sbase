package com.sbase.platform.bi;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Full physical inventory audit for Doris. This deliberately does not approve
 * tables: a scan can establish discriminator shape, not business ownership.
 */
@Service
public class DorisTenantGovernanceAuditService {
    private static final String UUID_PATTERN =
        "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$";
    private final JdbcTemplate postgres;
    private final JdbcTemplate doris;
    private final String database;

    public DorisTenantGovernanceAuditService(@Qualifier("jdbcTemplate") JdbcTemplate postgres,
            @Qualifier("dorisJdbcTemplate") JdbcTemplate doris,
            @Value("${sbase.doris.database:analytics}") String database) {
        this.postgres=postgres;
        this.doris=doris;
        if(database==null || !database.matches("[A-Za-z_][A-Za-z0-9_]*"))
            throw new IllegalArgumentException("Invalid Doris database identifier");
        this.database=database;
    }

    public Map<String,Object> auditDatabase(String subject) {
        if(subject==null || subject.isBlank()) throw new IllegalArgumentException("Audit subject is required");
        List<Map<String,Object>> physical=doris.queryForList("""
            SELECT TABLE_NAME AS table_name, TABLE_TYPE AS table_type
              FROM information_schema.tables
             WHERE TABLE_SCHEMA=?
             ORDER BY TABLE_NAME
            """,database);
        List<Map<String,Object>> results=new ArrayList<>();
        int verified=0,blocked=0,missingDiscriminator=0,unregistered=0;
        for(Map<String,Object> item:physical) {
            String table=Objects.toString(item.get("table_name"),"");
            if(!table.matches("[A-Za-z_][A-Za-z0-9_]*"))
                throw new IllegalStateException("Doris returned an unsafe table identifier");
            Integer columns=doris.queryForObject("""
                SELECT COUNT(*) FROM information_schema.columns
                 WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND LOWER(COLUMN_NAME)='tenant_id'
                """,Integer.class,database,table);
            boolean hasTenantColumn=columns!=null && columns==1;
            Boolean registered=postgres.query("""
                SELECT EXISTS(SELECT 1 FROM doris_tenant_table_policies
                 WHERE schema_name=? AND table_name=? AND enabled=true
                   AND tenant_column='tenant_id' AND classification='TENANT_DATA'
                   AND verification_status='VERIFIED' AND verified_at IS NOT NULL
                   AND invalid_tenant_rows=0 AND verified_row_count IS NOT NULL)
                """,rs->rs.next() && rs.getBoolean(1),database,table);
            long rowCount=0,invalidRows=-1;
            if(hasTenantColumn) {
                Map<String,Object> counts=doris.queryForMap(
                    "SELECT COUNT(*) AS row_count, " +
                    "COALESCE(SUM(CASE WHEN tenant_id IS NULL OR " +
                    "NOT CAST(tenant_id AS STRING) REGEXP ? THEN 1 ELSE 0 END),0) AS invalid_rows " +
                    "FROM `"+database+"`.`"+table+"`",UUID_PATTERN);
                rowCount=number(counts.get("row_count"));
                invalidRows=number(counts.get("invalid_rows"));
            }
            Boolean rowCountMatches=false;
            if(hasTenantColumn && Boolean.TRUE.equals(registered)) {
                rowCountMatches=postgres.query("""
                    SELECT EXISTS(SELECT 1 FROM doris_tenant_table_policies
                     WHERE schema_name=? AND table_name=? AND verified_row_count=? AND invalid_tenant_rows=0)
                    """,rs->rs.next() && rs.getBoolean(1),database,table,rowCount);
            }
            String status;
            if(!hasTenantColumn) { status="MISSING_TENANT_COLUMN"; missingDiscriminator++; blocked++; }
            else if(!Boolean.TRUE.equals(registered)) { status="UNREGISTERED_OR_UNVERIFIED"; unregistered++; blocked++; }
            else if(invalidRows!=0) { status="INVALID_TENANT_ROWS"; blocked++; }
            else if(!rowCountMatches) { status="STALE_VERIFICATION"; blocked++; }
            else { status="VERIFIED_SHAPE"; verified++; }
            if(!"VERIFIED_SHAPE".equals(status) && Boolean.TRUE.equals(registered)) {
                String rejectedStatus="INVALID_TENANT_ROWS".equals(status) || "MISSING_TENANT_COLUMN".equals(status)
                    ? "REJECTED" : "PENDING";
                if(invalidRows>=0) {
                    postgres.update("""
                        UPDATE doris_tenant_table_policies
                           SET enabled=FALSE, verification_status=?, invalid_tenant_rows=?,
                               verified_at=NULL, updated_at=now()
                         WHERE schema_name=? AND table_name=?
                        """,rejectedStatus,invalidRows,database,table);
                } else {
                    postgres.update("""
                        UPDATE doris_tenant_table_policies
                           SET enabled=FALSE, verification_status=?, verified_at=NULL, updated_at=now()
                         WHERE schema_name=? AND table_name=?
                        """,rejectedStatus,database,table);
                }
            }
            Map<String,Object> result=new LinkedHashMap<>();
            result.put("schema",database); result.put("table",table); result.put("objectType",Objects.toString(item.get("table_type"),"UNKNOWN")); result.put("status",status);
            result.put("rowCount",rowCount); result.put("invalidTenantRows",invalidRows);
            result.put("registered",Boolean.TRUE.equals(registered)); results.add(result);
            postgres.update("""
                INSERT INTO doris_tenant_policy_audit(schema_name,table_name,subject,action,outcome,details)
                VALUES(?,?,?,'INVENTORY_SCAN',?,?::jsonb)
                """,database,table,subject,status,
                jsonDetails(rowCount,invalidRows,hasTenantColumn,Boolean.TRUE.equals(registered)));
        }
        Map<String,Object> summary=new LinkedHashMap<>();
        summary.put("database",database); summary.put("tableCount",results.size());
        summary.put("verifiedShapeCount",verified); summary.put("blockedCount",blocked);
        summary.put("missingTenantColumnCount",missingDiscriminator);
        summary.put("unregisteredCount",unregistered); summary.put("tables",results);
        summary.put("complete",blocked==0); // Shape and registry only; ownership attestation remains an operator responsibility.
        return summary;
    }

    private static String jsonDetails(long rows,long invalid,boolean column,boolean registered) {
        return "{\"rowCount\":"+rows+",\"invalidTenantRows\":"+invalid+
            ",\"hasTenantColumn\":"+column+",\"registered\":"+registered+"}";
    }
    private static long number(Object value) { return value instanceof Number n?n.longValue():0L; }
}
