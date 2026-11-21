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
        try {
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
        var matcher=java.util.regex.Pattern.compile("(?i)(?:FROM|JOIN)\\s+([a-zA-Z_][\\w]*\\.[a-zA-Z_][\\w]*\\.[a-zA-Z_][\\w]*)").matcher(normalized);
        int refs=0;
        while(matcher.find()) {
            refs++;String[] id=matcher.group(1).split("\\.");
            if(!"sbase".equalsIgnoreCase(id[0])) throw new SecurityException("Only sbase workspace tables are queryable");
            control.requireTable(s.principal(),"sbase",id[1],id[2],LakehouseControlPlane.Privilege.SELECT);
        }
        if(normalized.matches("(?is).*\\b(FROM|JOIN)\\s+.*") && refs==0)
            throw new SecurityException("Table references must be fully qualified as sbase.namespace.table");
        int limit=Math.max(1,Math.min(requestedLimit,1000));
        try {
            Dataset<Row> frame=spark.sql(normalized).limit(limit);List<Map<String,Object>> rows=new ArrayList<>();
            for(String json:frame.toJSON().collectAsList()) rows.add(JSON.readValue(json,MAP));
            return Map.of("engine","Spark SQL / Iceberg","columns",Arrays.asList(frame.schema().fieldNames()),"rows",rows,"rowCount",rows.size(),"limit",limit);
        } catch(Exception e) { throw failure("Lakehouse SQL query failed",e); }
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
