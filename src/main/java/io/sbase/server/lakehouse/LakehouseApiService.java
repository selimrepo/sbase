package io.sbase.server.lakehouse;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.Base64;

/** JWT-authenticated workspace facade. Tenant identity is derived only from a verified HS256 token. */
public final class LakehouseApiService implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<Map<String,Object>> MAP = new TypeReference<>() {};
    private final SparkSession spark;
    private final HikariDataSource dataSource;
    private final LakehouseControlPlane control;
    private final String jwtSecret;
    public record Session(LakehouseControlPlane.Principal principal,String subject) {}

    public LakehouseApiService(SparkSession spark) {
        this.spark=Objects.requireNonNull(spark);
        this.jwtSecret=requiredEnv("SBASE_JWT_SECRET");
        HikariConfig config=new HikariConfig();
        config.setJdbcUrl(env("SBASE_POSTGRES_URL","jdbc:postgresql://localhost:5432/sbase"));
        config.setUsername(env("SBASE_POSTGRES_USER","sbase"));
        config.setPassword(env("SBASE_POSTGRES_PASSWORD","change-me"));
        config.setMaximumPoolSize(Integer.parseInt(env("SBASE_LAKEHOUSE_DB_POOL_SIZE","8")));
        config.setMinimumIdle(1);config.setPoolName("sbase-lakehouse-api");
        dataSource=new HikariDataSource(config);control=new LakehouseControlPlane(dataSource);
        SparkLineageListener.install(spark,control,LakehouseExecutionContext::workspaceId,LakehouseExecutionContext::principal);
    }

    public Session authenticate(String authorization) {
        if(authorization==null || !authorization.startsWith("Bearer ")) throw new SecurityException("Bearer access token required");
        String[] parts=authorization.substring(7).trim().split("\\.",-1);
        if(parts.length!=3) throw new SecurityException("Malformed access token");
        try {
            Map<String,Object> header=JSON.readValue(Base64.getUrlDecoder().decode(parts[0]),MAP);
            if(!"HS256".equals(header.get("alg"))) throw new SecurityException("Unsupported token algorithm");
            Mac mac=Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(jwtSecret.getBytes(StandardCharsets.UTF_8),"HmacSHA256"));
            byte[] expected=mac.doFinal((parts[0]+"."+parts[1]).getBytes(StandardCharsets.US_ASCII));
            if(!MessageDigest.isEqual(expected,Base64.getUrlDecoder().decode(parts[2]))) throw new SecurityException("Invalid access token signature");
            Map<String,Object> claims=JSON.readValue(Base64.getUrlDecoder().decode(parts[1]),MAP);
            long now=Instant.now().getEpochSecond();
            if(number(claims.get("exp"),0)<=now) throw new SecurityException("Access token expired");
            if(claims.containsKey("nbf") && number(claims.get("nbf"),Long.MAX_VALUE)>now) throw new SecurityException("Access token is not active");
            String subject=Objects.toString(claims.get("sub"),"").trim();
            String workspace=Objects.toString(claims.get("workspace_id"),"").trim();
            if(subject.isEmpty() || workspace.isEmpty()) throw new SecurityException("Token requires sub and workspace_id claims");
            Set<String> groups=new HashSet<>();
            if(claims.get("groups") instanceof Collection<?> values)
                values.forEach(v->{if(v instanceof String s && !s.isBlank()) groups.add(s);});
            String serviceId=claims.get("service_id") instanceof String s?s:null;
            return new Session(new LakehouseControlPlane.Principal(subject,UUID.fromString(workspace),groups,serviceId),subject);
        } catch(SecurityException e) { throw e; }
        catch(Exception e) { throw new SecurityException("Access token validation failed",e); }
    }

    public Map<String,Object> status(Session s) {
        return Map.of("status","UP","catalog","sbase","workspaceId",s.principal().workspaceId().toString(),
                "sparkVersion",spark.version(),"master",spark.sparkContext().master());
    }

    public List<Map<String,Object>> namespaces(Session s) {
        String sql="SELECT namespace_name,owner_principal,created_at FROM lakehouse_namespace_owner WHERE workspace_id=? ORDER BY namespace_name";
        try(var c=dataSource.getConnection();var p=c.prepareStatement(sql)) {
            p.setObject(1,s.principal().workspaceId());
            try(var r=p.executeQuery()) {
                List<Map<String,Object>> out=new ArrayList<>();
                while(r.next()) {
                    String name=r.getString(1);
                    try { control.requireNamespace(s.principal(),"sbase",name,LakehouseControlPlane.Privilege.USE); }
                    catch(SecurityException denied) { continue; }
                    out.add(Map.of("name",name,"owner",r.getString(2),"createdAt",r.getTimestamp(3).toInstant().toString()));
                }
                return out;
            }
        } catch(Exception e) { throw failure("Unable to list workspace namespaces",e); }
    }

    public Map<String,Object> createNamespace(Session s,String namespace) {
        identifier(namespace);spark.sql("CREATE NAMESPACE IF NOT EXISTS sbase."+namespace);
        String sql="INSERT INTO lakehouse_namespace_owner(catalog_name,namespace_name,workspace_id,owner_principal) VALUES('sbase',?,?,?) ON CONFLICT(catalog_name,namespace_name) DO NOTHING";
        try(var c=dataSource.getConnection();var p=c.prepareStatement(sql)) {
            p.setString(1,namespace);p.setObject(2,s.principal().workspaceId());p.setString(3,s.subject());
            if(p.executeUpdate()==0) try(var q=c.prepareStatement("SELECT workspace_id FROM lakehouse_namespace_owner WHERE catalog_name='sbase' AND namespace_name=?")) {
                q.setString(1,namespace);try(var r=q.executeQuery()) {
                    if(!r.next() || !s.principal().workspaceId().equals(r.getObject(1,UUID.class)))
                        throw new SecurityException("Namespace already belongs to another workspace");
                }
            }
            return Map.of("catalog","sbase","namespace",namespace,"status","AVAILABLE");
        } catch(SecurityException e) { throw e; }
        catch(Exception e) { throw failure("Unable to register namespace ownership",e); }
    }

    public List<Map<String,Object>> tables(Session s,String namespace) {
        identifier(namespace);control.requireNamespace(s.principal(),"sbase",namespace,LakehouseControlPlane.Privilege.USE);
        List<Map<String,Object>> out=new ArrayList<>();
        for(Row row:spark.sql("SHOW TABLES IN sbase."+namespace).collectAsList()) {
            String table=row.getAs("tableName");
            try { control.requireTable(s.principal(),"sbase",namespace,table,LakehouseControlPlane.Privilege.SELECT);
                out.add(Map.of("namespace",namespace,"name",table));
            } catch(SecurityException ignored) { }
        }
        return out;
    }

    public Map<String,Object> preview(Session s,String namespace,String table,int requestedLimit) {
        identifier(namespace);identifier(table);
        control.requireTable(s.principal(),"sbase",namespace,table,LakehouseControlPlane.Privilege.SELECT);
        try {
            int limit=Math.max(1,Math.min(requestedLimit,500));
            Dataset<Row> frame=spark.table("sbase."+namespace+"."+table).limit(limit);
            List<Map<String,Object>> rows=new ArrayList<>();
            for(String json:frame.toJSON().collectAsList()) rows.add(JSON.readValue(json,MAP));
            return Map.of("catalog","sbase","namespace",namespace,"table",table,"format","iceberg",
                    "columns",Arrays.asList(frame.schema().fieldNames()),"rows",rows,"rowCount",rows.size(),"limit",limit);
        } catch(Exception e) { throw failure("Unable to preview Iceberg table",e); }
    }

    public Map<String,Object> writeRows(Session s,String namespace,String table,List<Map<String,Object>> rows) {
        identifier(namespace);identifier(table);
        control.requireTable(s.principal(),"sbase",namespace,table,LakehouseControlPlane.Privilege.INSERT);
        if(rows==null || rows.isEmpty() || rows.size()>10000) throw new IllegalArgumentException("rows must contain 1..10000 records");
        try(LakehouseExecutionContext context=LakehouseExecutionContext.enter(s.principal().workspaceId(),s.subject())) {
            List<String> json=new ArrayList<>();for(Map<String,Object> row:rows) json.add(JSON.writeValueAsString(row));
            var rdd=new org.apache.spark.api.java.JavaSparkContext(spark.sparkContext()).parallelize(json,Math.min(8,json.size()));
            Dataset<Row> frame=spark.read().json(rdd);String id="sbase."+namespace+"."+table;
            boolean exists=spark.catalog().tableExists(id);
            if(exists) frame.writeTo(id).append();else frame.writeTo(id).using("iceberg").create();
            return Map.of("namespace",namespace,"table",table,"rowsWritten",rows.size(),"format","iceberg","mode",exists?"append":"create");
        } catch(Exception e) { throw failure("Iceberg write failed",e); }
    }

    /** Read-only SQL; all FROM/JOIN sources must be fully qualified workspace tables and authorized. */
    public Map<String,Object> query(Session s,String sql,int requestedLimit) {
        if(sql==null || sql.isBlank() || sql.length()>20000) throw new IllegalArgumentException("SQL is empty or too large");
        String normalized=sql.trim().replaceAll(";+$","").trim();
        if(!normalized.matches("(?is)^(SELECT|WITH)\\b.*") ||
           normalized.matches("(?is).*\\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE|CALL|SET|USE|CACHE|UNCACHE)\\b.*"))
            throw new SecurityException("SQL workspace accepts read-only SELECT/WITH statements only");
        String authorizationSql=normalized.replaceAll("(?s)/\\*.*?\\*/"," ")
                .replaceAll("(?m)--.*$"," ").replaceAll("'(?:''|[^'])*'","''");
        var relationTokens=java.util.regex.Pattern.compile("(?i)\\b(?:FROM|JOIN)\\b").matcher(authorizationSql);
        int relationCount=0;while(relationTokens.find()) relationCount++;
        var fromClause=java.util.regex.Pattern.compile("(?is)\\bFROM\\b(.*?)(?=\\bWHERE\\b|\\bGROUP\\s+BY\\b|\\bHAVING\\b|\\bORDER\\s+BY\\b|\\bLIMIT\\b|\\bQUALIFY\\b|\\bUNION\\b|\\bEXCEPT\\b|\\bINTERSECT\\b|$)").matcher(authorizationSql);
        while(fromClause.find()) {
            if(fromClause.group(1).contains(","))
                throw new SecurityException("Comma-separated FROM relations are not accepted; use explicit JOIN clauses");
        }
        Set<String> ctes=new HashSet<>();
        var cteMatcher=java.util.regex.Pattern.compile("(?i)(?:WITH|,)\\s*([A-Za-z_][\\w]*)\\s+AS\\s*\\(").matcher(authorizationSql);
        while(cteMatcher.find()) ctes.add(cteMatcher.group(1).toLowerCase(Locale.ROOT));
        var matcher=java.util.regex.Pattern.compile("(?i)\\b(?:FROM|JOIN)\\s+([A-Za-z_][\\w.]*)").matcher(authorizationSql);
        int matchedRelations=0;
        while(matcher.find()) {
            matchedRelations++;
            String[] id=matcher.group(1).split("\\.");
            if(id.length==1 && ctes.contains(id[0].toLowerCase(Locale.ROOT))) continue;
            if(id.length!=3 || !"sbase".equalsIgnoreCase(id[0]))
                throw new SecurityException("Every table reference must be a workspace table (sbase.namespace.table) or a declared CTE");
            control.requireTable(s.principal(),"sbase",id[1],id[2],LakehouseControlPlane.Privilege.SELECT);
        }
        if(matchedRelations!=relationCount)
            throw new SecurityException("Every FROM/JOIN relation must use an unquoted, fully qualified workspace table or a declared CTE");
        if(authorizationSql.matches("(?is).*\\b(FROM|JOIN)\\s*\\(.*"))
            throw new SecurityException("Inline subqueries in FROM/JOIN are not accepted; use a named CTE");
        int limit=Math.max(1,Math.min(requestedLimit,1000));
        try(LakehouseExecutionContext context=LakehouseExecutionContext.enter(s.principal().workspaceId(),s.subject())) {
            Dataset<Row> frame=spark.sql(normalized).limit(limit);List<Map<String,Object>> rows=new ArrayList<>();
            for(String json:frame.toJSON().collectAsList()) rows.add(JSON.readValue(json,MAP));
            return Map.of("engine","Spark SQL / Iceberg","columns",Arrays.asList(frame.schema().fieldNames()),"rows",rows,"rowCount",rows.size(),"limit",limit);
        } catch(Exception e) { throw failure("Lakehouse SQL query failed",e); }
    }

    public Map<String,Object> evolveSchema(Session s,String namespace,String table,Map<String,Object> body) {
        identifier(namespace);identifier(table);
        control.requireTable(s.principal(),"sbase",namespace,table,LakehouseControlPlane.Privilege.ALTER);
        String operation=Objects.toString(body.get("operation"),"").toUpperCase(Locale.ROOT);
        String identifier="sbase."+namespace+"."+table;
        LakehouseIngestionService ingestion=new LakehouseIngestionService(spark,ref->resolveSecret(ref));
        Map<String,Object> payload=new LinkedHashMap<>();
        String compatibility="BACKWARD";
        switch(operation) {
            case "ADD_COLUMN" -> {
                String ddl=required(body,"columnsDdl");
                ingestion.addColumns(identifier,ddl);payload.put("columnsDdl",ddl);
            }
            case "RENAME_COLUMN" -> {
                String oldName=identifier(Objects.toString(body.get("oldName"),""));
                String newName=identifier(Objects.toString(body.get("newName"),""));
                ingestion.renameColumn(identifier,oldName,newName);payload.put("oldName",oldName);payload.put("newName",newName);
            }
            case "TYPE_WIDEN" -> {
                String column=identifier(Objects.toString(body.get("column"),""));
                String targetType=required(body,"targetType").toUpperCase(Locale.ROOT);
                ingestion.widenColumnType(identifier,column,targetType);payload.put("column",column);payload.put("targetType",targetType);
            }
            default -> throw new IllegalArgumentException("Supported schema operations: ADD_COLUMN, RENAME_COLUMN, TYPE_WIDEN");
        }
        try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement(
                "INSERT INTO lakehouse_schema_change(id,workspace_id,catalog_name,namespace_name,table_name,change_type,change_payload,compatibility,recorded_by) VALUES(?,?,'sbase',?,?,?,?::jsonb,?,?)")) {
            p.setObject(1,UUID.randomUUID());p.setObject(2,s.principal().workspaceId());p.setString(3,namespace);p.setString(4,table);
            p.setString(5,operation);p.setString(6,JSON.writeValueAsString(payload));p.setString(7,compatibility);p.setString(8,s.subject());p.executeUpdate();
        } catch(Exception e) { throw failure("Schema changed but history persistence failed",e); }
        return Map.of("namespace",namespace,"table",table,"operation",operation,"compatibility",compatibility,"status","APPLIED");
    }

    public Map<String,Object> schema(Session s,String namespace,String table) {
        identifier(namespace);identifier(table);
        control.requireTable(s.principal(),"sbase",namespace,table,LakehouseControlPlane.Privilege.SELECT);
        Dataset<Row> frame=spark.table("sbase."+namespace+"."+table);
        return Map.of("catalog","sbase","namespace",namespace,"table",table,
                "schema",JSON.valueToTree(frame.schema()),"columns",Arrays.asList(frame.schema().fieldNames()));
    }

    public List<Map<String,Object>> snapshots(Session s,String namespace,String table) {
        identifier(namespace);identifier(table);
        control.requireTable(s.principal(),"sbase",namespace,table,LakehouseControlPlane.Privilege.SELECT);
        try {
            List<Map<String,Object>> out=new ArrayList<>();
            for(String json:spark.sql("SELECT * FROM sbase."+namespace+"."+table+".snapshots ORDER BY committed_at DESC LIMIT 100").toJSON().collectAsList())
                out.add(JSON.readValue(json,MAP));
            return out;
        } catch(Exception e) { throw failure("Unable to read Iceberg snapshots",e); }
    }

    public Map<String,Object> rollbackSnapshot(Session s,String namespace,String table,long snapshotId) {
        identifier(namespace);identifier(table);
        if(snapshotId<=0) throw new IllegalArgumentException("snapshotId must be positive");
        control.requireTable(s.principal(),"sbase",namespace,table,LakehouseControlPlane.Privilege.ALTER);
        try {
            String target=namespace+"."+table;
            List<Map<String,Object>> rows=new ArrayList<>();
            for(String json:spark.sql("CALL sbase.system.rollback_to_snapshot(table => '"+target+"', snapshot_id => "+snapshotId+")").toJSON().collectAsList())
                rows.add(JSON.readValue(json,MAP));
            return Map.of("namespace",namespace,"table",table,"snapshotId",snapshotId,"status","ROLLED_BACK","details",rows);
        } catch(Exception e) { throw failure("Iceberg snapshot rollback failed",e); }
    }

    public Map<String,Object> compact(Session s,String namespace,String table) {
        identifier(namespace);identifier(table);
        control.requireTable(s.principal(),"sbase",namespace,table,LakehouseControlPlane.Privilege.MODIFY);
        new IcebergMaintenanceService(spark).compact("sbase",namespace,table,128);
        return Map.of("namespace",namespace,"table",table,"status","COMPACTION_COMPLETED");
    }

    public Map<String,Object> createMaintenancePolicy(Session s,String namespace,String table,String operation,int intervalSeconds,
                                                        int retentionDays,int orphanHours) {
        identifier(namespace);identifier(table);
        control.requireTable(s.principal(),"sbase",namespace,table,LakehouseControlPlane.Privilege.MODIFY);
        Set<String> allowed=Set.of("COMPACT","REWRITE_MANIFESTS","EXPIRE_SNAPSHOTS","REMOVE_ORPHAN_FILES");
        if(!allowed.contains(operation)) throw new IllegalArgumentException("Unsupported maintenance operation");
        if(intervalSeconds<60 || intervalSeconds>31536000 || retentionDays<1 || orphanHours<24)
            throw new IllegalArgumentException("Invalid maintenance schedule or retention");
        UUID id=UUID.randomUUID();
        String sql="INSERT INTO lakehouse_maintenance_policy(id,workspace_id,catalog_name,namespace_name,table_name,operation,cron_expression,interval_seconds,retention_days,orphan_older_than_hours,next_run_at,created_by) VALUES(?,?,'sbase',?,?,?,'interval',?,?,?,?,?)";
        try(var c=dataSource.getConnection();var p=c.prepareStatement(sql)) {
            p.setObject(1,id);p.setObject(2,s.principal().workspaceId());p.setString(3,namespace);p.setString(4,table);
            p.setString(5,operation);p.setInt(6,intervalSeconds);p.setInt(7,retentionDays);p.setInt(8,orphanHours);
            p.setTimestamp(9,java.sql.Timestamp.from(Instant.now()));p.setString(10,s.subject());p.executeUpdate();
            return Map.of("policyId",id.toString(),"namespace",namespace,"table",table,"operation",operation,
                    "intervalSeconds",intervalSeconds,"status","ENABLED");
        } catch(Exception e) { throw failure("Unable to create maintenance policy",e); }
    }

    public List<Map<String,Object>> maintenanceHistory(Session s,String namespace,int requestedLimit) {
        control.requireNamespace(s.principal(),"sbase",namespace,LakehouseControlPlane.Privilege.USE);
        int limit=Math.max(1,Math.min(requestedLimit,500));
        String sql="SELECT r.id,r.policy_id,r.status,r.started_at,r.finished_at,r.summary::text,r.error_message,p.table_name,p.operation FROM lakehouse_maintenance_run r JOIN lakehouse_maintenance_policy p ON p.id=r.policy_id WHERE p.workspace_id=? AND p.namespace_name=? ORDER BY r.started_at DESC LIMIT ?";
        try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement(sql)) {
            p.setObject(1,s.principal().workspaceId());p.setString(2,namespace);p.setInt(3,limit);
            try(ResultSet r=p.executeQuery()) {
                List<Map<String,Object>> out=new ArrayList<>();
                while(r.next()) {
                    Map<String,Object> row=new LinkedHashMap<>();
                    row.put("id",r.getObject(1).toString());row.put("policyId",r.getObject(2).toString());row.put("status",r.getString(3));
                    row.put("startedAt",r.getTimestamp(4).toInstant().toString());
                    row.put("finishedAt",r.getTimestamp(5)==null?null:r.getTimestamp(5).toInstant().toString());
                    row.put("summary",JSON.readTree(r.getString(6)));row.put("error",r.getString(7));
                    row.put("table",r.getString(8));row.put("operation",r.getString(9));out.add(row);
                }
                return out;
            }
        } catch(Exception e) { throw failure("Unable to list maintenance history",e); }
    }

    public Map<String,Object> setMaintenancePolicyEnabled(Session s,UUID policyId,boolean enabled) {
        String namespace;String table;
        try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement(
                "SELECT namespace_name,table_name FROM lakehouse_maintenance_policy WHERE id=? AND workspace_id=?")) {
            p.setObject(1,policyId);p.setObject(2,s.principal().workspaceId());
            try(ResultSet r=p.executeQuery()) {
                if(!r.next()) throw new NoSuchElementException("Maintenance policy not found in active workspace");
                namespace=r.getString(1);table=r.getString(2);
            }
        } catch(NoSuchElementException e) { throw e; }
        catch(Exception e) { throw failure("Unable to authorize maintenance policy update",e); }
        control.requireTable(s.principal(),"sbase",namespace,table,LakehouseControlPlane.Privilege.MODIFY);
        try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement(
                "UPDATE lakehouse_maintenance_policy SET enabled=?,next_run_at=CASE WHEN ? THEN now() ELSE next_run_at END,lease_owner=NULL,lease_until=NULL WHERE id=? AND workspace_id=?")) {
            p.setBoolean(1,enabled);p.setBoolean(2,enabled);p.setObject(3,policyId);p.setObject(4,s.principal().workspaceId());
            if(p.executeUpdate()!=1) throw new NoSuchElementException("Maintenance policy not found");
            return Map.of("policyId",policyId.toString(),"enabled",enabled);
        } catch(NoSuchElementException e) { throw e; }
        catch(Exception e) { throw failure("Unable to update maintenance policy",e); }
    }

    public List<Map<String,Object>> maintenancePolicies(Session s,String namespace) {
        control.requireNamespace(s.principal(),"sbase",namespace,LakehouseControlPlane.Privilege.USE);
        String sql="SELECT id,table_name,operation,interval_seconds,retention_days,orphan_older_than_hours,enabled,next_run_at FROM lakehouse_maintenance_policy WHERE workspace_id=? AND namespace_name=? ORDER BY created_at DESC";
        try(var c=dataSource.getConnection();var p=c.prepareStatement(sql)) {
            p.setObject(1,s.principal().workspaceId());p.setString(2,namespace);
            try(var r=p.executeQuery()) {
                List<Map<String,Object>> out=new ArrayList<>();
                while(r.next()) out.add(Map.of("id",r.getObject(1).toString(),"table",Objects.toString(r.getString(2),""),"operation",r.getString(3),
                        "intervalSeconds",r.getInt(4),"retentionDays",r.getInt(5),"orphanOlderThanHours",r.getInt(6),
                        "enabled",r.getBoolean(7),"nextRunAt",Objects.toString(r.getTimestamp(8),"")));
                return out;
            }
        } catch(Exception e) { throw failure("Unable to list maintenance policies",e); }
    }

    public Map<String,Object> createPipeline(Session s,Map<String,Object> request) {
        String name=Objects.toString(request.get("name"),"").trim();
        if(name.isEmpty() || name.length()>200) throw new IllegalArgumentException("Pipeline name is required (max 200 chars)");
        String sourceType=Objects.toString(request.get("sourceType"),"").toUpperCase(Locale.ROOT);
        if(!Set.of("S3","JDBC","KAFKA").contains(sourceType)) throw new IllegalArgumentException("sourceType must be S3, JDBC or KAFKA");
        String namespace=identifier(Objects.toString(request.get("targetNamespace"),""));
        String table=identifier(Objects.toString(request.get("targetTable"),""));
        String engine=Objects.toString(request.getOrDefault("engine","SPARK")).toUpperCase(Locale.ROOT);
        String mode=Objects.toString(request.getOrDefault("mode","BATCH")).toUpperCase(Locale.ROOT);
        if(!Set.of("SPARK","FLINK").contains(engine) || !Set.of("BATCH","STREAMING","CDC").contains(mode))
            throw new IllegalArgumentException("Unsupported engine or ingestion mode");
        if("FLINK".equals(engine) && "BATCH".equals(mode)) throw new IllegalArgumentException("Flink pipelines must use STREAMING or CDC mode");
        if("SPARK".equals(engine) && !"BATCH".equals(mode)) throw new IllegalArgumentException("Spark ingestion currently supports BATCH mode; use Flink for STREAMING or CDC");
        control.requireNamespace(s.principal(),"sbase",namespace,LakehouseControlPlane.Privilege.CREATE);
        Object sourceObject=request.get("source");
        if(!(sourceObject instanceof Map<?,?> sourceRaw)) throw new IllegalArgumentException("source object is required");
        Map<String,Object> source=new LinkedHashMap<>();
        sourceRaw.forEach((k,v)->{ if(k instanceof String key) {
            String lower=key.toLowerCase(Locale.ROOT);
            if(lower.contains("password") || lower.contains("secret") || lower.contains("token") || lower.contains("credential"))
                throw new IllegalArgumentException("Store credentials in the secret provider; pipeline configuration accepts secretRef only");
            source.put(key,v);
        }});
        String secretRef=Objects.toString(request.get("secretRef"),"");
        if("JDBC".equals(sourceType) && secretRef.isBlank()) throw new IllegalArgumentException("JDBC source requires secretRef");
        Map<String,Object> config=request.get("configuration") instanceof Map<?,?> m ? stringMap(m) : Map.of();
        if("FLINK".equals(engine)) {
            if(!"KAFKA".equals(sourceType))
                throw new IllegalArgumentException("Flink streaming currently accepts Kafka as its managed source");
            if(Objects.toString(config.get("flinkJarId"),"").isBlank())
                throw new IllegalArgumentException("Flink streaming requires configuration.flinkJarId");
            String sourceDdl=Objects.toString(config.get("sourceDdl"),"").trim();
            String insertSql=Objects.toString(config.get("insertSql"),"").trim();
            if(sourceDdl.length()>20000 || insertSql.length()>20000 ||
               sourceDdl.contains(";") || insertSql.contains(";") ||
               sourceDdl.contains("--") || insertSql.contains("--") ||
               sourceDdl.contains("/*") || insertSql.contains("/*"))
                throw new IllegalArgumentException("Flink SQL must be a single bounded statement without comments or statement separators");
            if(!sourceDdl.matches("(?is)^CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?[A-Za-z_][A-Za-z0-9_]*\\s*\\(.*\\)\\s*WITH\\s*\\(.*\\)$") ||
               !sourceDdl.matches("(?is).*['\\\"]connector['\\\"]\\s*=\\s*['\\\"]kafka['\\\"].*"))
                throw new IllegalArgumentException("Flink source DDL must define one unqualified Kafka source table");
            if(!insertSql.matches("(?is)^INSERT\\s+INTO\\s+sbase_iceberg\\."+java.util.regex.Pattern.quote(namespace)+"\\."+java.util.regex.Pattern.quote(table)+"(?:\\s|\\().*"))
                throw new IllegalArgumentException("Flink INSERT must target this pipeline's exact Iceberg table");
            String sensitive="(?i).*(password|secret|access[._-]?key|token|credential|sasl\\.jaas\\.config).*";
            if(config.keySet().stream().anyMatch(k->k.matches(sensitive)) ||
               sourceDdl.matches(sensitive) || insertSql.matches(sensitive))
                throw new IllegalArgumentException("Do not embed credentials in Flink SQL or pipeline configuration; use cluster secret configuration");
            if(!Objects.toString(request.getOrDefault("checkpointUri","")).matches("(?i)^s3a?://.+"))
                throw new IllegalArgumentException("Flink requires a durable s3:// or s3a:// checkpointUri");
        }
        UUID sourceId=UUID.randomUUID(),pipelineId=UUID.randomUUID();
        Map<String,Object> definition=new LinkedHashMap<>();
        definition.put("sourceType",sourceType);definition.put("source",source);definition.put("secretRef",secretRef);
        definition.put("targetCatalog","sbase");definition.put("targetNamespace",namespace);definition.put("targetTable",table);
        definition.put("engine",engine);definition.put("mode",mode);definition.put("configuration",config);
        definition.put("createdBy",s.subject());
        definition.put("checkpointUri",Objects.toString(request.getOrDefault("checkpointUri","")));
        try {
            String sourceJson=JSON.writeValueAsString(source);
            String definitionJson=JSON.writeValueAsString(definition);
            String checksum=HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(definitionJson.getBytes(StandardCharsets.UTF_8)));
            try(Connection c=dataSource.getConnection()) {
                c.setAutoCommit(false);
                try(PreparedStatement p=c.prepareStatement("INSERT INTO lakehouse_data_source(id,workspace_id,name,source_type,configuration,secret_ref,created_by) VALUES(?,?,?,?,?::jsonb,?,?)")) {
                    p.setObject(1,sourceId);p.setObject(2,s.principal().workspaceId());p.setString(3,name+" source");
                    p.setString(4,sourceType);p.setString(5,sourceJson);p.setString(6,secretRef.isBlank()?null:secretRef);p.setString(7,s.subject());p.executeUpdate();
                }
                try(PreparedStatement p=c.prepareStatement("INSERT INTO lakehouse_ingestion_pipeline(id,workspace_id,name,source_id,target_catalog,target_namespace,target_table,engine,mode,configuration,checkpoint_uri,max_retries,created_by) VALUES(?,?,?,?, 'sbase',?,?,?,?,?::jsonb,?,?,?)")) {
                    p.setObject(1,pipelineId);p.setObject(2,s.principal().workspaceId());p.setString(3,name);p.setObject(4,sourceId);
                    p.setString(5,namespace);p.setString(6,table);p.setString(7,engine);p.setString(8,mode);
                    p.setString(9,JSON.writeValueAsString(config));p.setString(10,Objects.toString(request.get("checkpointUri"),null));
                    p.setInt(11,Math.max(0,Math.min(100,((Number)request.getOrDefault("maxRetries",5)).intValue())));p.setString(12,s.subject());p.executeUpdate();
                }
                try(PreparedStatement p=c.prepareStatement("INSERT INTO lakehouse_pipeline_version(pipeline_id,version,definition,checksum,created_by) VALUES(?,1,?::jsonb,?,?)")) {
                    p.setObject(1,pipelineId);p.setString(2,definitionJson);p.setString(3,checksum);p.setString(4,s.subject());p.executeUpdate();
                }
                c.commit();
            }
            return Map.of("pipelineId",pipelineId.toString(),"sourceId",sourceId.toString(),"name",name,
                    "target","sbase."+namespace+"."+table,"engine",engine,"mode",mode,"version",1,"status","ENABLED");
        } catch(Exception e) { throw failure("Unable to create ingestion pipeline",e); }
    }

    public List<Map<String,Object>> pipelines(Session s) {
        String sql="SELECT id,name,source_id,engine,mode,target_namespace,target_table,enabled,active_version,created_at,updated_at FROM lakehouse_ingestion_pipeline WHERE workspace_id=? ORDER BY updated_at DESC";
        try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement(sql)) {
            p.setObject(1,s.principal().workspaceId());
            try(ResultSet r=p.executeQuery()) {
                List<Map<String,Object>> out=new ArrayList<>();
                while(r.next()) {
                    Map<String,Object> item=new LinkedHashMap<>();
                    item.put("id",r.getObject(1).toString());item.put("name",r.getString(2));item.put("sourceId",r.getObject(3).toString());
                    item.put("engine",r.getString(4));item.put("mode",r.getString(5));item.put("targetNamespace",r.getString(6));item.put("targetTable",r.getString(7));
                    item.put("enabled",r.getBoolean(8));item.put("version",r.getInt(9));item.put("createdAt",r.getTimestamp(10).toInstant().toString());
                    item.put("updatedAt",r.getTimestamp(11).toInstant().toString());out.add(item);
                }
                return out;
            }
        } catch(Exception e) { throw failure("Unable to list ingestion pipelines",e); }
    }

    public Map<String,Object> runPipeline(Session s,UUID pipelineId,String idempotencyKey) {
        if(idempotencyKey==null || idempotencyKey.isBlank() || idempotencyKey.length()>240)
            throw new IllegalArgumentException("idempotencyKey is required (max 240 chars)");
        try(Connection c=dataSource.getConnection()) {
            c.setAutoCommit(false);
            UUID workspace=s.principal().workspaceId();
            String select="SELECT active_version,max_retries FROM lakehouse_ingestion_pipeline WHERE id=? AND workspace_id=? AND enabled=true FOR UPDATE";
            int version,maxAttempts;
            try(PreparedStatement p=c.prepareStatement(select)) {
                p.setObject(1,pipelineId);p.setObject(2,workspace);
                try(ResultSet r=p.executeQuery()) { if(!r.next()) throw new NoSuchElementException("Pipeline not found");version=r.getInt(1);maxAttempts=r.getInt(2)+1; }
            }
            UUID attempt=UUID.randomUUID();
            try(PreparedStatement p=c.prepareStatement("INSERT INTO lakehouse_pipeline_attempt(id,pipeline_id,pipeline_version,attempt,status,idempotency_key) VALUES(?,?,?,0,'QUEUED',?) ON CONFLICT(pipeline_id,idempotency_key) DO NOTHING")) {
                p.setObject(1,attempt);p.setObject(2,pipelineId);p.setInt(3,version);p.setString(4,idempotencyKey);
                if(p.executeUpdate()==0) {
                    try(PreparedStatement q=c.prepareStatement("SELECT id,status FROM lakehouse_pipeline_attempt WHERE pipeline_id=? AND idempotency_key=?")) {
                        q.setObject(1,pipelineId);q.setString(2,idempotencyKey);
                        try(ResultSet r=q.executeQuery()) { r.next();UUID existing=r.getObject(1,UUID.class);String status=r.getString(2);c.commit();return Map.of("attemptId",existing.toString(),"status",status,"idempotentReplay",true); }
                    }
                }
            }
            UUID runId=UUID.randomUUID();
            try(PreparedStatement p=c.prepareStatement("INSERT INTO lakehouse_ingestion_run(id,workspace_id,pipeline_id,pipeline_attempt_id,target_namespace,target_table,engine,status) SELECT ?,workspace_id,id,?,target_namespace,target_table,engine,'QUEUED' FROM lakehouse_ingestion_pipeline WHERE id=?")) {
                p.setObject(1,runId);p.setObject(2,attempt);p.setObject(3,pipelineId);p.executeUpdate();
            }
            try(PreparedStatement p=c.prepareStatement("UPDATE lakehouse_pipeline_attempt SET ingestion_run_id=? WHERE id=?")) {
                p.setObject(1,runId);p.setObject(2,attempt);p.executeUpdate();
            }
            c.commit();
            return Map.of("attemptId",attempt.toString(),"ingestionRunId",runId.toString(),"status","QUEUED","idempotentReplay",false);
        } catch(NoSuchElementException e) { throw e; }
        catch(Exception e) { throw failure("Unable to queue ingestion run",e); }
    }

    public Map<String,Object> cancelPipelineRun(Session s,UUID pipelineId,UUID attemptId) {
        String status,flinkJobId;
        UUID runId;
        try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement(
                "SELECT a.status,a.runtime_job_id,a.ingestion_run_id FROM lakehouse_pipeline_attempt a JOIN lakehouse_ingestion_pipeline i ON i.id=a.pipeline_id WHERE a.id=? AND a.pipeline_id=? AND i.workspace_id=?")) {
            p.setObject(1,attemptId);p.setObject(2,pipelineId);p.setObject(3,s.principal().workspaceId());
            try(ResultSet r=p.executeQuery()) {
                if(!r.next()) throw new NoSuchElementException("Pipeline run not found");
                status=r.getString(1);flinkJobId=r.getString(2);runId=r.getObject(3,UUID.class);
            }
        } catch(NoSuchElementException e) { throw e; }
        catch(Exception e) { throw failure("Unable to inspect pipeline run",e); }
        if(Set.of("SUCCEEDED","FAILED","CANCELLED").contains(status))
            return Map.of("attemptId",attemptId.toString(),"status",status,"cancelRequested",false);
        if(flinkJobId!=null && !flinkJobId.isBlank()) {
            try {
                new io.sbase.server.runtime.PlatformRuntimeService(spark).flinkCancel(flinkJobId);
                return Map.of("attemptId",attemptId.toString(),"flinkJobId",flinkJobId,"status","CANCEL_REQUESTED","cancelRequested",true);
            } catch(Exception e) { throw failure("Unable to cancel Flink job",e); }
        }
        if(!"QUEUED".equals(status) && !"RETRY_WAIT".equals(status))
            throw new IllegalStateException("A running Spark batch job cannot be cancelled through this endpoint");
        try(Connection c=dataSource.getConnection()) {
            c.setAutoCommit(false);
            try(PreparedStatement p=c.prepareStatement("UPDATE lakehouse_pipeline_attempt SET status='CANCELLED',finished_at=now() WHERE id=? AND status IN ('QUEUED','RETRY_WAIT')")) {
                p.setObject(1,attemptId);if(p.executeUpdate()==0) { c.rollback();throw new IllegalStateException("Pipeline run was claimed while cancellation was requested"); }
            }
            try(PreparedStatement p=c.prepareStatement("UPDATE lakehouse_ingestion_run SET status='CANCELLED',finished_at=now() WHERE id=? AND workspace_id=?")) {
                p.setObject(1,runId);p.setObject(2,s.principal().workspaceId());p.executeUpdate();
            }
            c.commit();
            return Map.of("attemptId",attemptId.toString(),"status","CANCELLED","cancelRequested",true);
        } catch(Exception e) { throw failure("Unable to cancel queued pipeline run",e); }
    }

    public List<Map<String,Object>> pipelineRuns(Session s,UUID pipelineId,int requestedLimit) {
        int limit=Math.max(1,Math.min(requestedLimit,200));
        String sql="SELECT a.id,a.pipeline_version,a.attempt,a.status,a.submitted_at,a.started_at,a.finished_at,a.error_class,a.error_message,a.runtime_job_id,a.result::text FROM lakehouse_pipeline_attempt a JOIN lakehouse_ingestion_pipeline p ON p.id=a.pipeline_id WHERE p.workspace_id=? AND p.id=? ORDER BY a.submitted_at DESC LIMIT ?";
        try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement(sql)) {
            p.setObject(1,s.principal().workspaceId());p.setObject(2,pipelineId);p.setInt(3,limit);
            try(ResultSet r=p.executeQuery()) {
                List<Map<String,Object>> out=new ArrayList<>();
                while(r.next()) {
                    Map<String,Object> item=new LinkedHashMap<>();item.put("id",r.getObject(1).toString());item.put("version",r.getInt(2));item.put("attempt",r.getInt(3));
                    item.put("status",r.getString(4));item.put("submittedAt",r.getTimestamp(5).toInstant().toString());
                    item.put("startedAt",r.getTimestamp(6)==null?null:r.getTimestamp(6).toInstant().toString());
                    item.put("finishedAt",r.getTimestamp(7)==null?null:r.getTimestamp(7).toInstant().toString());
                    item.put("errorClass",r.getString(8));item.put("errorMessage",r.getString(9));item.put("runtimeJobId",r.getString(10));
                    try { item.put("result",JSON.readTree(r.getString(11))); } catch(Exception ignored) { item.put("result",Map.of()); }
                    out.add(item);
                }
                return out;
            }
        } catch(Exception e) { throw failure("Unable to list pipeline runs",e); }
    }

    private static Map<String,Object> stringMap(Map<?,?> raw) {
        Map<String,Object> out=new LinkedHashMap<>();raw.forEach((k,v)->{if(k instanceof String key)out.put(key,v);});return out;
    }

    public Map<String,Object> grantNamespace(Session s,String namespace,Map<String,Object> body) {
        control.requireNamespace(s.principal(),"sbase",namespace,LakehouseControlPlane.Privilege.OWNERSHIP);
        var type=LakehouseControlPlane.PrincipalType.valueOf(Objects.toString(body.get("principalType"),"USER").toUpperCase(Locale.ROOT));
        var privilege=LakehouseControlPlane.Privilege.valueOf(Objects.toString(body.get("privilege"),"USE").toUpperCase(Locale.ROOT));
        String principalId=required(body,"principalId");
        Instant expires=body.get("expiresAt") instanceof String v && !v.isBlank()?Instant.parse(v):null;
        control.grantNamespace("sbase",namespace,type,principalId,privilege,s.subject(),expires);
        return Map.of("namespace",namespace,"principalType",type.name(),"principalId",principalId,"privilege",privilege.name(),"status","GRANTED");
    }

    public Map<String,Object> grantTable(Session s,String namespace,String table,Map<String,Object> body) {
        control.requireNamespace(s.principal(),"sbase",namespace,LakehouseControlPlane.Privilege.OWNERSHIP);
        identifier(table);
        var type=LakehouseControlPlane.PrincipalType.valueOf(Objects.toString(body.get("principalType"),"USER").toUpperCase(Locale.ROOT));
        var privilege=LakehouseControlPlane.Privilege.valueOf(Objects.toString(body.get("privilege"),"SELECT").toUpperCase(Locale.ROOT));
        String principalId=required(body,"principalId");
        Instant expires=body.get("expiresAt") instanceof String v && !v.isBlank()?Instant.parse(v):null;
        control.grantTable("sbase",namespace,table,type,principalId,privilege,s.subject(),expires);
        return Map.of("namespace",namespace,"table",table,"principalType",type.name(),"principalId",principalId,"privilege",privilege.name(),"status","GRANTED");
    }

    public Map<String,Object> revokeGrant(Session s,String namespace,String table,Map<String,Object> body) {
        control.requireNamespace(s.principal(),"sbase",namespace,LakehouseControlPlane.Privilege.OWNERSHIP);
        var type=LakehouseControlPlane.PrincipalType.valueOf(Objects.toString(body.get("principalType"),"USER").toUpperCase(Locale.ROOT));
        var privilege=LakehouseControlPlane.Privilege.valueOf(Objects.toString(body.get("privilege"),"SELECT").toUpperCase(Locale.ROOT));
        String principalId=required(body,"principalId");
        boolean changed;
        if(table==null || table.isBlank()) changed=control.revokeNamespace("sbase",namespace,type,principalId,privilege);
        else changed=control.revokeTable("sbase",namespace,identifier(table),type,principalId,privilege);
        return Map.of("revoked",changed,"namespace",namespace,"table",Objects.toString(table,""));
    }

    public List<Map<String,Object>> grants(Session s,String namespace) {
        control.requireNamespace(s.principal(),"sbase",namespace,LakehouseControlPlane.Privilege.OWNERSHIP);
        String sql="""
          SELECT principal_type,principal_id,privilege,'NAMESPACE' AS scope,NULL AS table_name,
                 granted_at,expires_at,revoked_at FROM lakehouse_namespace_grant
          WHERE catalog_name='sbase' AND namespace_name=?
          UNION ALL
          SELECT principal_type,principal_id,privilege,'TABLE' AS scope,table_name,
                 granted_at,expires_at,revoked_at FROM lakehouse_table_grant
          WHERE catalog_name='sbase' AND namespace_name=?
          ORDER BY scope,table_name,principal_type,principal_id,privilege
        """;
        try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement(sql)) {
            p.setString(1,namespace);p.setString(2,namespace);
            try(ResultSet r=p.executeQuery()) {
                List<Map<String,Object>> out=new ArrayList<>();
                while(r.next()) {
                    Map<String,Object> item=new LinkedHashMap<>();
                    item.put("principalType",r.getString(1));item.put("principalId",r.getString(2));
                    item.put("privilege",r.getString(3));item.put("scope",r.getString(4));item.put("table",r.getString(5));
                    item.put("grantedAt",r.getTimestamp(6).toInstant().toString());
                    item.put("expiresAt",r.getTimestamp(7)==null?null:r.getTimestamp(7).toInstant().toString());
                    item.put("revokedAt",r.getTimestamp(8)==null?null:r.getTimestamp(8).toInstant().toString());out.add(item);
                }
                return out;
            }
        } catch(Exception e) { throw failure("Unable to list namespace grants",e); }
    }

    public List<Map<String,Object>> lineage(Session s,String namespace,int requestedLimit) {
        control.requireNamespace(s.principal(),"sbase",namespace,LakehouseControlPlane.Privilege.USE);
        int limit=Math.max(1,Math.min(requestedLimit,1000));
        String sql="SELECT source_catalog,source_namespace,source_table,target_catalog,target_namespace,target_table,relation_type,recorded_by,recorded_at,details::text FROM lakehouse_lineage_edge WHERE workspace_id=? AND (source_namespace=? OR target_namespace=?) ORDER BY recorded_at DESC LIMIT ?";
        try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement(sql)) {
            p.setObject(1,s.principal().workspaceId());p.setString(2,namespace);p.setString(3,namespace);p.setInt(4,limit);
            try(ResultSet r=p.executeQuery()) {
                List<Map<String,Object>> out=new ArrayList<>();
                while(r.next()) out.add(Map.of("source",r.getString(1)+"."+r.getString(2)+"."+r.getString(3),
                        "target",r.getString(4)+"."+r.getString(5)+"."+r.getString(6),"relation",r.getString(7),
                        "recordedBy",r.getString(8),"recordedAt",r.getTimestamp(9).toInstant().toString(),"details",JSON.readTree(r.getString(10))));
                return out;
            }
        } catch(Exception e) { throw failure("Unable to list workspace lineage",e); }
    }

    public Map<String,Object> commitCdcOffset(Session s,UUID sourceId,String stream,String partition,String offsetJson,long expectedGeneration) {
        try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement(
                "SELECT 1 FROM lakehouse_data_source WHERE id=? AND workspace_id=? AND enabled=true")) {
            p.setObject(1,sourceId);p.setObject(2,s.principal().workspaceId());
            try(ResultSet r=p.executeQuery()) { if(!r.next()) throw new SecurityException("Data source is outside the active workspace"); }
        } catch(SecurityException e) { throw e; }
        catch(Exception e) { throw failure("Unable to authorize CDC source",e); }
        try {
            JSON.readTree(offsetJson);
            boolean committed=control.commitCdcOffset(s.principal().workspaceId(),sourceId,stream,partition,offsetJson,expectedGeneration);
            return Map.of("committed",committed,"sourceId",sourceId.toString(),"stream",stream,"partition",partition);
        } catch(Exception e) { throw failure("Unable to commit CDC offset",e); }
    }

    private static String resolveSecret(String ref) {
        if(ref==null || ref.isBlank()) throw new IllegalArgumentException("secretRef is required");
        String envKey="SBASE_SECRET_"+ref.toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]","_");
        String value=System.getenv(envKey);
        if(value==null) throw new IllegalStateException("Secret provider did not resolve "+ref+" (expected "+envKey+")");
        return value;
    }

    private static String required(Map<String,?> body,String key) {
        String value=Objects.toString(body.get(key),"").trim();
        if(value.isEmpty() || value.length()>500) throw new IllegalArgumentException(key+" is required (max 500 chars)");
        return value;
    }

    public List<Map<String,Object>> deadLetters(Session s,UUID runId,int requestedLimit) {
        requireRunWorkspace(s,runId);
        int limit=Math.max(1,Math.min(requestedLimit,1000));
        String sql="SELECT id,source_partition,source_offset::text,record_key,payload::text,error_class,error_message,attempts,status,first_seen_at FROM lakehouse_dead_letter WHERE workspace_id=? AND ingestion_run_id=? ORDER BY first_seen_at LIMIT ?";
        try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement(sql)) {
            p.setObject(1,s.principal().workspaceId());p.setObject(2,runId);p.setInt(3,limit);
            try(ResultSet r=p.executeQuery()) {
                List<Map<String,Object>> out=new ArrayList<>();
                while(r.next()) {
                    Map<String,Object> item=new LinkedHashMap<>();item.put("id",r.getObject(1).toString());item.put("partition",r.getString(2));
                    item.put("offset",JSON.readTree(r.getString(3)));item.put("recordKey",r.getString(4));item.put("payload",JSON.readTree(r.getString(5)));
                    item.put("errorClass",r.getString(6));item.put("errorMessage",r.getString(7));item.put("attempts",r.getInt(8));
                    item.put("status",r.getString(9));item.put("firstSeenAt",r.getTimestamp(10).toInstant().toString());out.add(item);
                }
                return out;
            }
        } catch(Exception e) { throw failure("Unable to list dead-letter records",e); }
    }

    public List<Map<String,Object>> claimDeadLetters(Session s,UUID runId,int limit) {
        requireRunWorkspace(s,runId);
        List<Map<String,Object>> out=new ArrayList<>();
        for(LakehouseControlPlane.DeadLetter d:control.claimDeadLetters(s.principal().workspaceId(),runId,Math.max(1,Math.min(limit,1000)))) {
            Map<String,Object> item=new LinkedHashMap<>();item.put("id",d.id().toString());item.put("partition",d.partition());
            try { item.put("offset",JSON.readTree(d.offsetJson()));item.put("payload",JSON.readTree(d.payloadJson())); }
            catch(Exception e) { throw failure("Dead-letter payload is invalid JSON",e); }
            item.put("recordKey",d.recordKey());item.put("errorClass",d.errorClass());item.put("errorMessage",d.errorMessage());
            item.put("attempts",d.attempts());out.add(item);
        }
        return out;
    }

    public Map<String,Object> finishDeadLetter(Session s,UUID deadLetterId,boolean resolved) {
        try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement(
                "SELECT 1 FROM lakehouse_dead_letter WHERE id=? AND workspace_id=?")) {
            p.setObject(1,deadLetterId);p.setObject(2,s.principal().workspaceId());
            try(ResultSet r=p.executeQuery()) { if(!r.next()) throw new SecurityException("Dead-letter record is outside the active workspace"); }
        } catch(SecurityException e) { throw e; }
        catch(Exception e) { throw failure("Unable to authorize dead-letter update",e); }
        control.finishDeadLetter(deadLetterId,resolved);
        return Map.of("id",deadLetterId.toString(),"status",resolved?"RESOLVED":"PENDING");
    }

    private void requireRunWorkspace(Session s,UUID runId) {
        try(Connection c=dataSource.getConnection();PreparedStatement p=c.prepareStatement(
                "SELECT 1 FROM lakehouse_ingestion_run WHERE id=? AND workspace_id=?")) {
            p.setObject(1,runId);p.setObject(2,s.principal().workspaceId());
            try(ResultSet r=p.executeQuery()) { if(!r.next()) throw new SecurityException("Ingestion run is outside the active workspace"); }
        } catch(SecurityException e) { throw e; }
        catch(Exception e) { throw failure("Unable to authorize ingestion run",e); }
    }

    private static byte[] decode(String v) { return Base64.getUrlDecoder().decode(v); }
    private static long number(Object v,long fallback) { return v instanceof Number n?n.longValue():fallback; }
    private static String identifier(String v) {
        if(v==null || !v.matches("[A-Za-z_][A-Za-z0-9_]{0,127}")) throw new IllegalArgumentException("Invalid SQL identifier");
        return v;
    }
    private static String env(String k,String d) { return Optional.ofNullable(System.getenv(k)).filter(v->!v.isBlank()).orElse(d); }
    private static String requiredEnv(String k) { String v=System.getenv(k);if(v==null||v.length()<32)throw new IllegalStateException(k+" must be configured with at least 32 characters");return v; }
    private static IllegalStateException failure(String m,Exception e) { return new IllegalStateException(m+": "+e.getMessage(),e); }
    @Override public void close() { dataSource.close(); }
}
