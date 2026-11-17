package io.sbase.server.catalog;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.SolrQuery;

import java.net.InetSocketAddress;
import java.sql.*;
import java.util.*;

/** Read-only federation facade for the PostgreSQL, Cassandra and Solr catalogs. */
public final class CatalogService implements AutoCloseable {
    private final String pgUrl = env("SBASE_POSTGRES_URL", "jdbc:postgresql://localhost:5432/sbase");
    private final String pgUser = env("SBASE_POSTGRES_USER", "sbase");
    private final String pgPassword = env("SBASE_POSTGRES_PASSWORD", "");
    private final String cassandraContact = env("SBASE_CASSANDRA_CONTACT_POINT", "localhost");
    private final int cassandraPort = Integer.parseInt(env("SBASE_CASSANDRA_PORT", "9042"));
    private final String cassandraDatacenter = env("SBASE_CASSANDRA_DATACENTER", "datacenter1");
    private final String solrUrl = env("SBASE_SOLR_URL", "http://localhost:8983/solr");
    private volatile CqlSession cassandra;
    private volatile SolrClient solr;

    private static String env(String key, String fallback) {
        return Optional.ofNullable(System.getenv(key)).filter(s -> !s.isBlank()).orElse(fallback);
    }

    public Map<String,Object> catalogs() {
        Map<String,Object> result = new LinkedHashMap<>();
        result.put("postgresql", postgresCatalog());
        result.put("cassandra", cassandraCatalog());
        result.put("solr", solrCatalog());
        return result;
    }

    private Map<String,Object> postgresCatalog() {
        try (Connection c = DriverManager.getConnection(pgUrl, pgUser, pgPassword);
             ResultSet rs = c.getMetaData().getTables(null, "public", "%", new String[]{"TABLE","VIEW"})) {
            List<Map<String,String>> tables = new ArrayList<>();
            while (rs.next()) tables.add(Map.of("schema", rs.getString("TABLE_SCHEM"),
                    "name", rs.getString("TABLE_NAME"), "type", rs.getString("TABLE_TYPE")));
            return Map.of("status","UP","tables",tables);
        } catch (Exception e) { return Map.of("status","UNAVAILABLE","error",safe(e)); }
    }

    private Map<String,Object> cassandraCatalog() {
        try {
            CqlSession s = cassandra();
            List<String> keyspaces = s.getMetadata().getKeyspaces().keySet().stream()
                    .map(Object::toString).sorted().toList();
            return Map.of("status","UP","keyspaces",keyspaces);
        } catch (Exception e) { return Map.of("status","UNAVAILABLE","error",safe(e)); }
    }

    private Map<String,Object> solrCatalog() {
        try {
            SolrQuery q = new SolrQuery("*:*"); q.setRows(0);
            QueryResponse r = solr().query("_default", q);
            return Map.of("status","UP","defaultCoreAvailable",r.getStatus()==0);
        } catch (Exception e) { return Map.of("status","UNAVAILABLE","error",safe(e)); }
    }

    /** Executes a bounded, read-only PostgreSQL SELECT with JDBC bind parameters. */
    public Map<String,Object> queryPostgres(String sql, List<Object> parameters, int limit) throws SQLException {
        if(sql==null||sql.isBlank())throw new IllegalArgumentException("SQL query is required");
        SqlNode parsed;
        try{parsed=SqlParser.create(sql).parseQuery();}
        catch(Exception invalid){throw new IllegalArgumentException("Invalid or non-read-only SQL query",invalid);}
        if(parsed.getKind()!=SqlKind.SELECT&&parsed.getKind()!=SqlKind.WITH)
            throw new IllegalArgumentException("Only read-only SELECT queries are allowed");
        int bounded = Math.max(1, Math.min(limit, 5000));
        try (Connection c = DriverManager.getConnection(pgUrl, pgUser, pgPassword)) {
            c.setReadOnly(true);
            try (PreparedStatement st = c.prepareStatement(sql)) {
                st.setQueryTimeout(30);
                for (int i=0;i<parameters.size();i++) st.setObject(i+1, parameters.get(i));
                try (ResultSet rs = st.executeQuery()) {
                    ResultSetMetaData md=rs.getMetaData(); List<Map<String,Object>> rows=new ArrayList<>();
                    int count=0;
                    while (rs.next() && count++<bounded) {
                        Map<String,Object> row=new LinkedHashMap<>();
                        for(int col=1;col<=md.getColumnCount();col++) row.put(md.getColumnLabel(col),rs.getObject(col));
                        rows.add(row);
                    }
                    return Map.of("rows",rows,"rowCount",rows.size(),"limit",bounded);
                }
            }
        }
    }

    public Map<String,Object> queryCassandra(String cql, int limit) {
        if(cql==null||!cql.stripLeading().toLowerCase(Locale.ROOT).startsWith("select "))
            throw new IllegalArgumentException("Only Cassandra SELECT is allowed");
        if(cql.contains(";")||cql.contains("--"))throw new IllegalArgumentException("Only one CQL statement is allowed");
        int bounded=Math.max(1,Math.min(limit,2000));
        List<Map<String,Object>> rows=new ArrayList<>();
        var statement=SimpleStatement.builder(cql).setPageSize(bounded).build();
        var iterator=cassandra().execute(statement).iterator();
        while(iterator.hasNext()&&rows.size()<bounded){
            Row row=iterator.next();Map<String,Object> values=new LinkedHashMap<>();
            row.getColumnDefinitions().forEach(def->{
                Object value=row.getObject(def.getName());
                if(value!=null)values.put(def.getName().asInternal(),
                        value instanceof Number||value instanceof Boolean?value:value.toString());
            });
            rows.add(values);
        }
        return Map.of("rows",rows,"rowCount",rows.size(),"limit",bounded);
    }

    public Map<String,Object> querySolr(String collection, String expression, int limit) throws Exception {
        if(collection==null || !collection.matches("[A-Za-z0-9_.-]{1,128}"))
            throw new IllegalArgumentException("Invalid Solr collection name");
        SolrQuery q=new SolrQuery(expression==null||expression.isBlank()?"*:*":expression);
        q.setRows(Math.max(1,Math.min(limit,2000)));
        QueryResponse response=solr().query(collection,q);
        return Map.of("documents",response.getResults(),"numFound",response.getResults().getNumFound());
    }

    /** Returns column metadata for a single PostgreSQL table without executing user SQL. */
    public Map<String,Object> postgresColumns(String schema, String table) throws SQLException {
        if (schema == null || !schema.matches("[A-Za-z_][A-Za-z0-9_]{0,62}")
                || table == null || !table.matches("[A-Za-z_][A-Za-z0-9_]{0,62}"))
            throw new IllegalArgumentException("Invalid PostgreSQL schema or table name");
        try (Connection c = DriverManager.getConnection(pgUrl, pgUser, pgPassword)) {
            DatabaseMetaData metadata = c.getMetaData();
            List<Map<String,Object>> columns = new ArrayList<>();
            try (ResultSet rs = metadata.getColumns(null, schema, table, "%")) {
                while (rs.next()) {
                    Map<String,Object> column = new LinkedHashMap<>();
                    column.put("name", rs.getString("COLUMN_NAME"));
                    column.put("type", rs.getString("TYPE_NAME"));
                    column.put("jdbcType", rs.getInt("DATA_TYPE"));
                    column.put("size", rs.getInt("COLUMN_SIZE"));
                    column.put("nullable", rs.getInt("NULLABLE") != DatabaseMetaData.columnNoNulls);
                    column.put("ordinal", rs.getInt("ORDINAL_POSITION"));
                    column.put("default", rs.getString("COLUMN_DEF"));
                    columns.add(column);
                }
            }
            if (columns.isEmpty()) throw new NoSuchElementException("PostgreSQL table not found: " + schema + "." + table);
            return Map.of("source", "postgresql", "schema", schema, "table", table, "columns", columns);
        }
    }

    private CqlSession cassandra() {
        if(cassandra==null) synchronized(this) {
            if(cassandra==null) cassandra=CqlSession.builder()
                    .addContactPoint(new InetSocketAddress(cassandraContact,cassandraPort))
                    .withLocalDatacenter(cassandraDatacenter).build();
        }
        return cassandra;
    }
    private SolrClient solr() {
        if(solr==null) synchronized(this) { if(solr==null) solr=new HttpSolrClient.Builder(solrUrl).build(); }
        return solr;
    }
    private static String safe(Exception e) {
        return e.getClass().getSimpleName()+": "+Optional.ofNullable(e.getMessage()).orElse("connection failed");
    }
    @Override public void close() throws Exception {
        if(cassandra!=null) cassandra.close();
        if(solr!=null) solr.close();
    }
}
