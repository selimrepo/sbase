package io.sbase.server.lakehouse;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.*;

/**
 * PostgreSQL-backed lakehouse control plane.
 *
 * Iceberg remains authoritative for physical table metadata. This service is the
 * mandatory authorization and operational metadata gate in front of catalog access.
 */
public final class LakehouseControlPlane {
    public enum Privilege { USE, CREATE, SELECT, MODIFY, DROP, OWNERSHIP, INSERT, UPDATE, DELETE, ALTER }
    public enum PrincipalType { USER, GROUP, WORKSPACE, SERVICE }

    public record Principal(String userId, UUID workspaceId, Set<String> groups, String serviceId) {
        public Principal {
            Objects.requireNonNull(userId, "userId");
            Objects.requireNonNull(workspaceId, "workspaceId");
            groups = groups == null ? Set.of() : Set.copyOf(groups);
        }
    }

    public record DeadLetter(UUID id, UUID ingestionRunId, String partition, String offsetJson,
                             String recordKey, String payloadJson, String errorClass,
                             String errorMessage, int attempts) {}

    private final DataSource dataSource;

    public LakehouseControlPlane(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    public void requireNamespace(Principal principal, String catalog, String namespace, Privilege privilege) {
        Objects.requireNonNull(principal, "principal");
        String sql = """
            SELECT EXISTS (
              SELECT 1 FROM lakehouse_namespace_owner o
              WHERE o.catalog_name=? AND o.namespace_name=? AND o.workspace_id=?
                AND (o.owner_principal=? OR EXISTS (
                  SELECT 1 FROM lakehouse_namespace_grant g
                  WHERE g.catalog_name=o.catalog_name AND g.namespace_name=o.namespace_name
                    AND g.privilege=? AND g.revoked_at IS NULL
                    AND (g.expires_at IS NULL OR g.expires_at > now())
                    AND (
                      (g.principal_type='USER' AND g.principal_id=?) OR
                      (g.principal_type='WORKSPACE' AND g.principal_id=?) OR
                      (g.principal_type='SERVICE' AND g.principal_id=?) OR
                      (g.principal_type='GROUP' AND g.principal_id = ANY(?))
                    )
                ))
            )
            """;
        if (hasNamespaceGrant(principal, catalog, namespace, privilege, sql)) return;
        throw new SecurityException("Missing " + privilege + " privilege on " + catalog + "." + namespace);
    }

    public void requireTable(Principal principal, String catalog, String namespace,
                             String table, Privilege privilege) {
        requireNamespace(principal, catalog, namespace, Privilege.USE);
        String tablePrivilege = switch (privilege) {
            case SELECT -> "SELECT";
            case INSERT -> "INSERT";
            case UPDATE -> "UPDATE";
            case DELETE -> "DELETE";
            case ALTER -> "ALTER";
            case DROP -> "DROP";
            case OWNERSHIP -> "OWNERSHIP";
            default -> null;
        };
        if (tablePrivilege != null && hasTableGrant(principal, catalog, namespace, table, tablePrivilege)) return;
        Privilege namespaceFallback = switch (privilege) {
            case SELECT -> Privilege.SELECT;
            case INSERT, UPDATE, DELETE, MODIFY -> Privilege.MODIFY;
            case ALTER -> Privilege.MODIFY;
            case DROP, OWNERSHIP -> Privilege.OWNERSHIP;
            default -> privilege;
        };
        requireNamespace(principal, catalog, namespace, namespaceFallback);
    }

    private void requireNamespaceExistsInWorkspace(Principal p, String catalog, String namespace) {
        String sql = "SELECT 1 FROM lakehouse_namespace_owner WHERE catalog_name=? AND namespace_name=? AND workspace_id=?";
        try (Connection c=dataSource.getConnection(); PreparedStatement s=c.prepareStatement(sql)) {
            s.setString(1,catalog); s.setString(2,namespace); s.setObject(3,p.workspaceId());
            try (ResultSet r=s.executeQuery()) {
                if (!r.next()) throw new SecurityException("Namespace is not owned by the active workspace");
            }
        } catch (SQLException e) { throw new IllegalStateException("Unable to resolve lakehouse namespace",e); }
    }

    private boolean hasNamespaceGrant(Principal p, String catalog, String namespace, Privilege privilege, String sql) {
        try (Connection c=dataSource.getConnection(); PreparedStatement s=c.prepareStatement(sql)) {
            s.setString(1,catalog); s.setString(2,namespace); s.setObject(3,p.workspaceId());
            s.setString(4,p.userId()); s.setString(5,privilege.name()); s.setString(6,p.userId());
            s.setString(7,p.workspaceId().toString()); s.setString(8,Objects.toString(p.serviceId(),""));
            Array groups=c.createArrayOf("text",p.groups().toArray(String[]::new)); s.setArray(9,groups);
            try (ResultSet r=s.executeQuery()) { return r.next() && r.getBoolean(1); }
        } catch (SQLException e) { throw new IllegalStateException("Unable to evaluate namespace grants",e); }
    }

    private boolean hasTableGrant(Principal p, String catalog, String namespace, String table, String privilege) {
        String sql = """
            SELECT EXISTS (
              SELECT 1 FROM lakehouse_table_grant g
              JOIN lakehouse_namespace_owner o
                ON o.catalog_name=g.catalog_name AND o.namespace_name=g.namespace_name
              WHERE g.catalog_name=? AND g.namespace_name=? AND g.table_name=?
                AND o.workspace_id=? AND g.privilege=? AND g.revoked_at IS NULL
                AND (g.expires_at IS NULL OR g.expires_at > now())
                AND (
                  (g.principal_type='USER' AND g.principal_id=?) OR
                  (g.principal_type='WORKSPACE' AND g.principal_id=?) OR
                  (g.principal_type='SERVICE' AND g.principal_id=?) OR
                  (g.principal_type='GROUP' AND g.principal_id = ANY(?))
                )
            )
            """;
        try (Connection c=dataSource.getConnection(); PreparedStatement s=c.prepareStatement(sql)) {
            s.setString(1,catalog); s.setString(2,namespace); s.setString(3,table);
            s.setObject(4,p.workspaceId()); s.setString(5,privilege);
            s.setString(6,p.userId()); s.setString(7,p.workspaceId().toString());
            s.setString(8,Objects.toString(p.serviceId(),""));
            Array groups=c.createArrayOf("text",p.groups().toArray(String[]::new));
            s.setArray(9,groups);
            try (ResultSet r=s.executeQuery()) { return r.next() && r.getBoolean(1); }
        } catch (SQLException e) { throw new IllegalStateException("Unable to evaluate table grants",e); }
    }

    public void grantNamespace(String catalog, String namespace, PrincipalType type, String principalId,
                               Privilege privilege, String grantedBy, Instant expiresAt) {
        String sql = """
            INSERT INTO lakehouse_namespace_grant
              (id,catalog_name,namespace_name,principal_type,principal_id,privilege,granted_by,expires_at)
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT (catalog_name,namespace_name,principal_type,principal_id,privilege)
            DO UPDATE SET granted_by=EXCLUDED.granted_by, granted_at=now(),
              expires_at=EXCLUDED.expires_at, revoked_at=NULL
            """;
        try (Connection c=dataSource.getConnection(); PreparedStatement s=c.prepareStatement(sql)) {
            s.setObject(1,UUID.randomUUID()); s.setString(2,catalog); s.setString(3,namespace);
            s.setString(4,type.name()); s.setString(5,principalId); s.setString(6,privilege.name());
            s.setString(7,grantedBy); s.setTimestamp(8,expiresAt==null?null:Timestamp.from(expiresAt)); s.executeUpdate();
        } catch (SQLException e) { throw new IllegalStateException("Unable to grant namespace privilege",e); }
    }

    public void grantTable(String catalog, String namespace, String table, PrincipalType type, String principalId,
                           Privilege privilege, String grantedBy, Instant expiresAt) {
        String sql = """
            INSERT INTO lakehouse_table_grant
              (id,catalog_name,namespace_name,table_name,principal_type,principal_id,privilege,granted_by,expires_at)
            VALUES (?,?,?,?,?,?,?,?,?)
            ON CONFLICT (catalog_name,namespace_name,table_name,principal_type,principal_id,privilege)
            DO UPDATE SET granted_by=EXCLUDED.granted_by, granted_at=now(),
              expires_at=EXCLUDED.expires_at, revoked_at=NULL
            """;
        try (Connection c=dataSource.getConnection(); PreparedStatement s=c.prepareStatement(sql)) {
            s.setObject(1,UUID.randomUUID()); s.setString(2,catalog); s.setString(3,namespace);
            s.setString(4,table); s.setString(5,type.name()); s.setString(6,principalId);
            s.setString(7,privilege.name()); s.setString(8,grantedBy);
            s.setTimestamp(9,expiresAt==null?null:Timestamp.from(expiresAt)); s.executeUpdate();
        } catch (SQLException e) { throw new IllegalStateException("Unable to grant table privilege",e); }
    }

    public boolean revokeNamespace(String catalog, String namespace, PrincipalType type, String principalId, Privilege privilege) {
        return revoke("UPDATE lakehouse_namespace_grant SET revoked_at=now() WHERE catalog_name=? AND namespace_name=? AND principal_type=? AND principal_id=? AND privilege=? AND revoked_at IS NULL",
            catalog,namespace,type.name(),principalId,privilege.name());
    }

    public boolean revokeTable(String catalog, String namespace, String table, PrincipalType type, String principalId, Privilege privilege) {
        return revoke("UPDATE lakehouse_table_grant SET revoked_at=now() WHERE catalog_name=? AND namespace_name=? AND table_name=? AND principal_type=? AND principal_id=? AND privilege=? AND revoked_at IS NULL",
            catalog,namespace,table,type.name(),principalId,privilege.name());
    }

    private boolean revoke(String sql, String... values) {
        try (Connection c=dataSource.getConnection(); PreparedStatement s=c.prepareStatement(sql)) {
            for(int i=0;i<values.length;i++) s.setString(i+1,values[i]);
            return s.executeUpdate()==1;
        } catch (SQLException e) { throw new IllegalStateException("Unable to revoke lakehouse grant",e); }
    }

    public void recordLineage(UUID workspaceId, String sourceCatalog, String sourceNamespace, String sourceTable,
                              String targetCatalog, String targetNamespace, String targetTable,
                              String relationType, UUID jobId, UUID runId, String principal, String detailsJson) {
        String sql = """
            INSERT INTO lakehouse_lineage_edge
              (id,workspace_id,source_catalog,source_namespace,source_table,target_catalog,target_namespace,target_table,
               relation_type,job_id,run_id,recorded_by,details)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,CAST(? AS jsonb))
            ON CONFLICT DO NOTHING
            """;
        try (Connection c=dataSource.getConnection(); PreparedStatement s=c.prepareStatement(sql)) {
            s.setObject(1,UUID.randomUUID()); s.setObject(2,workspaceId);
            s.setString(3,sourceCatalog); s.setString(4,sourceNamespace); s.setString(5,sourceTable);
            s.setString(6,targetCatalog); s.setString(7,targetNamespace); s.setString(8,targetTable);
            s.setString(9,relationType); s.setObject(10,jobId); s.setObject(11,runId);
            s.setString(12,principal); s.setString(13,detailsJson==null?"{}":detailsJson); s.executeUpdate();
        } catch (SQLException e) { throw new IllegalStateException("Unable to persist table lineage",e); }
    }

    /** Compare-and-set CDC checkpoint: offsets cannot move backwards by racing writers. */
    public boolean commitCdcOffset(UUID workspaceId, UUID sourceId, String stream, String partition,
                                   String offsetJson, long expectedGeneration) {
        String select = "SELECT generation FROM lakehouse_cdc_offset WHERE workspace_id=? AND source_id=? AND stream_name=? AND partition_key=? FOR UPDATE";
        String insert = "INSERT INTO lakehouse_cdc_offset(workspace_id,source_id,stream_name,partition_key,committed_offset,generation) VALUES (?,?,?,?,CAST(? AS jsonb),1)";
        String update = "UPDATE lakehouse_cdc_offset SET committed_offset=CAST(? AS jsonb),generation=generation+1,updated_at=now() WHERE workspace_id=? AND source_id=? AND stream_name=? AND partition_key=? AND generation=?";
        try (Connection c=dataSource.getConnection()) {
            c.setAutoCommit(false);
            try {
                Long current=null;
                try (PreparedStatement q=c.prepareStatement(select)) {
                    q.setObject(1,workspaceId); q.setObject(2,sourceId); q.setString(3,stream); q.setString(4,partition);
                    try (ResultSet r=q.executeQuery()) { if(r.next()) current=r.getLong(1); }
                }
                if(current==null && expectedGeneration!=0) { c.rollback(); return false; }
                if(current!=null && current!=expectedGeneration) { c.rollback(); return false; }
                if(current==null) {
                    try (PreparedStatement s=c.prepareStatement(insert)) {
                        s.setObject(1,workspaceId); s.setObject(2,sourceId); s.setString(3,stream);
                        s.setString(4,partition); s.setString(5,offsetJson); s.executeUpdate();
                    }
                } else {
                    try (PreparedStatement s=c.prepareStatement(update)) {
                        s.setString(1,offsetJson); s.setObject(2,workspaceId); s.setObject(3,sourceId);
                        s.setString(4,stream); s.setString(5,partition); s.setLong(6,expectedGeneration);
                        if(s.executeUpdate()!=1) { c.rollback(); return false; }
                    }
                }
                c.commit(); return true;
            } catch(SQLException e) {
                c.rollback();
                if("23505".equals(e.getSQLState())) return false;
                throw e;
            }
        } catch(SQLException e) { throw new IllegalStateException("Unable to commit CDC offset",e); }
    }

    public UUID enqueueDeadLetter(UUID workspaceId, UUID runId, String partition, String offsetJson,
                                  String key, String payloadJson, String errorClass, String errorMessage) {
        UUID id=UUID.randomUUID();
        String sql = """
            INSERT INTO lakehouse_dead_letter
              (id,workspace_id,ingestion_run_id,source_partition,source_offset,record_key,payload,error_class,error_message)
            VALUES (?,?,?, ?,CAST(? AS jsonb),?,CAST(? AS jsonb),?,?)
            """;
        try (Connection c=dataSource.getConnection(); PreparedStatement s=c.prepareStatement(sql)) {
            s.setObject(1,id); s.setObject(2,workspaceId); s.setObject(3,runId);
            s.setString(4,partition); s.setString(5,offsetJson); s.setString(6,key);
            s.setString(7,payloadJson); s.setString(8,errorClass); s.setString(9,errorMessage);
            s.executeUpdate(); return id;
        } catch (SQLException e) { throw new IllegalStateException("Unable to enqueue dead-letter record",e); }
    }

    /** Claims a bounded replay batch without allowing two workers to own the same record. */
    public List<DeadLetter> claimDeadLetters(UUID workspaceId, UUID runId, int limit) {
        if(limit<1 || limit>1000) throw new IllegalArgumentException("limit must be between 1 and 1000");
        String sql = """
            WITH picked AS (
              SELECT id FROM lakehouse_dead_letter
              WHERE workspace_id=? AND ingestion_run_id=? AND status='PENDING'
              ORDER BY first_seen_at LIMIT ? FOR UPDATE SKIP LOCKED
            )
            UPDATE lakehouse_dead_letter d SET status='REPLAYING', attempts=attempts+1, last_seen_at=now()
            FROM picked WHERE d.id=picked.id
            RETURNING d.id,d.ingestion_run_id,d.source_partition,d.source_offset::text,d.record_key,
                      d.payload::text,d.error_class,d.error_message,d.attempts
            """;
        try (Connection c=dataSource.getConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement s=c.prepareStatement(sql)) {
                s.setObject(1,workspaceId); s.setObject(2,runId); s.setInt(3,limit);
                List<DeadLetter> result=new ArrayList<>();
                try(ResultSet r=s.executeQuery()) {
                    while(r.next()) result.add(new DeadLetter(r.getObject(1,UUID.class),r.getObject(2,UUID.class),
                        r.getString(3),r.getString(4),r.getString(5),r.getString(6),r.getString(7),r.getString(8),r.getInt(9)));
                }
                c.commit(); return List.copyOf(result);
            } catch(Exception e) { c.rollback(); throw e; }
        } catch(SQLException e) { throw new IllegalStateException("Unable to claim dead-letter batch",e); }
    }

    public void finishDeadLetter(UUID id, boolean resolved) {
        String sql = "UPDATE lakehouse_dead_letter SET status=?,resolved_at=CASE WHEN ? THEN now() ELSE NULL END WHERE id=? AND status='REPLAYING'";
        try(Connection c=dataSource.getConnection();PreparedStatement s=c.prepareStatement(sql)) {
            s.setString(1,resolved?"RESOLVED":"PENDING"); s.setBoolean(2,resolved); s.setObject(3,id);
            if(s.executeUpdate()!=1) throw new IllegalStateException("Dead-letter record is not claimed");
        } catch(SQLException e) { throw new IllegalStateException("Unable to finish dead-letter replay",e); }
    }
}
