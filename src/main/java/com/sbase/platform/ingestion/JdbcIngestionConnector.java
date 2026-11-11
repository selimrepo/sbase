package com.sbase.platform.ingestion;

import com.sbase.platform.IngestionConnector;
import org.springframework.stereotype.Component;
import java.sql.*;
import java.time.*;
import java.util.*;

@Component
public class JdbcIngestionConnector implements IngestionConnector {
    private static final int BATCH_SIZE = 1000;
    private final DurableRecordSink checkpoints;

    public JdbcIngestionConnector(DurableRecordSink checkpoints) { this.checkpoints = checkpoints; }
    @Override public String type() { return "jdbc"; }

    @Override
    public ConnectorResult ingest(ConnectorRequest request) {
        Map<String,String> o = request.options();
        String query = required(o,"query");
        String target = safeIdentifier(required(o,"targetTable"));
        if (!query.stripLeading().toLowerCase(Locale.ROOT).startsWith("select"))
            throw new IllegalArgumentException("Only SELECT source queries are allowed");

        String mode = o.getOrDefault("loadMode","full").toLowerCase(Locale.ROOT);
        if (!Set.of("full","incremental").contains(mode)) throw new IllegalArgumentException("loadMode must be full or incremental");
        boolean incremental = mode.equals("incremental");
        String watermarkColumn = o.get("watermarkColumn");
        if (incremental && (watermarkColumn == null || watermarkColumn.isBlank() || !query.contains("{watermark}")))
            throw new IllegalArgumentException("Incremental mode requires watermarkColumn and exactly one {watermark} placeholder");
        if (incremental && query.indexOf("{watermark}") != query.lastIndexOf("{watermark}"))
            throw new IllegalArgumentException("Only one {watermark} placeholder is supported");
        if (!incremental && query.contains("{watermark}")) throw new IllegalArgumentException("{watermark} is only valid in incremental mode");

        UUID connectorId = UUID.fromString(required(o,"connectorId"));
        String checkpointKey = "jdbc:" + target + ":" + watermarkColumn;
        String watermark = incremental ? checkpoints.checkpoint(connectorId,checkpointKey) : null;
        if (incremental && (watermark == null || watermark.isBlank())) watermark = required(o,"initialWatermark");
        String executableQuery = incremental ? query.replace("{watermark}","?") : query;
        long read=0,written=0;
        Object maxWatermark=null;

        try (Connection source = DriverManager.getConnection(required(o,"jdbcUrl"),o.getOrDefault("username",""),o.getOrDefault("password",""));
             PreparedStatement select = source.prepareStatement(executableQuery)) {
            if (incremental) select.setObject(1,parseWatermark(watermark,o.getOrDefault("watermarkType","string")));
            try (ResultSet rs=select.executeQuery()) {
                ResultSetMetaData md=rs.getMetaData(); int n=md.getColumnCount();
                int watermarkIndex=incremental?findColumn(md,watermarkColumn):-1;
                StringJoiner columns=new StringJoiner(",");
                for(int i=1;i<=n;i++) columns.add(safeIdentifier(md.getColumnLabel(i)));
                String insert="INSERT INTO "+target+" ("+columns+") VALUES ("+String.join(",",Collections.nCopies(n,"?"))+")";
                try(Connection dest=DriverManager.getConnection(required(o,"targetJdbcUrl"),o.getOrDefault("targetUsername",""),o.getOrDefault("targetPassword",""));
                    PreparedStatement ps=dest.prepareStatement(insert)) {
                    dest.setAutoCommit(false);
                    try {
                        while(rs.next()) {
                            for(int i=1;i<=n;i++) ps.setObject(i,rs.getObject(i));
                            ps.addBatch(); read++;
                            if(incremental) {
                                Object current=rs.getObject(watermarkIndex);
                                if(current!=null && (maxWatermark==null || compare(current,maxWatermark)>0)) maxWatermark=current;
                            }
                            if(read%BATCH_SIZE==0) written+=successful(ps.executeBatch());
                        }
                        written+=successful(ps.executeBatch());
                        dest.commit();
                    } catch(Exception ex) { dest.rollback(); throw ex; }
                }
            }
        } catch(SQLException ex) { throw new IllegalStateException("JDBC ingestion failed",ex); }

        // Advance only after the destination transaction commits. A crash before this update replays
        // the previous window; target-side unique keys/upserts should make that replay idempotent.
        if(incremental && maxWatermark!=null) checkpoints.saveCheckpoint(connectorId,checkpointKey,maxWatermark.toString());
        return new ConnectorResult(read,written,incremental
            ? "Incremental JDBC load completed; watermark="+(maxWatermark==null?watermark:maxWatermark)
            : "Full JDBC load completed");
    }

    private static int findColumn(ResultSetMetaData md,String name)throws SQLException {
        for(int i=1;i<=md.getColumnCount();i++) if(md.getColumnLabel(i).equalsIgnoreCase(name)) return i;
        throw new IllegalArgumentException("watermarkColumn must be selected: "+name);
    }
    private static int successful(int[] counts) { return (int)Arrays.stream(counts).filter(x->x>0||x==Statement.SUCCESS_NO_INFO).count(); }
    private static Object parseWatermark(String value,String type) {
        return switch(type.toLowerCase(Locale.ROOT)) {
            case "long","integer" -> Long.valueOf(value);
            case "decimal" -> new java.math.BigDecimal(value);
            case "timestamp" -> Timestamp.from(Instant.parse(value));
            case "date" -> Date.valueOf(LocalDate.parse(value));
            default -> value;
        };
    }
    @SuppressWarnings({"rawtypes","unchecked"})
    private static int compare(Object a,Object b) {
        if(a instanceof Comparable c && a.getClass().isInstance(b)) return c.compareTo(b);
        return a.toString().compareTo(b.toString());
    }
    private static int successfulCount(int[] counts) { return successful(counts); }
    private static String safeIdentifier(String s) {
        if(!s.matches("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?")) throw new IllegalArgumentException("Unsafe SQL identifier: "+s);
        return s;
    }
    private static String required(Map<String,String> o,String k) {
        String v=o.get(k); if(v==null||v.isBlank()) throw new IllegalArgumentException("Missing option: "+k); return v;
    }
}
