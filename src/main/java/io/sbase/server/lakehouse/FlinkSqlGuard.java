package io.sbase.server.lakehouse;

import java.util.Locale;
import java.util.regex.Pattern;

/** Central allow-list for the user-supplied Flink SQL surface. */
public final class FlinkSqlGuard {
    private static final Pattern SOURCE_DDL=Pattern.compile(
            "(?is)^CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?[A-Za-z_][A-Za-z0-9_]*\\s*\\(.*\\)\\s*WITH\\s*\\(.*\\)$");
    private static final Pattern KAFKA_CONNECTOR=Pattern.compile(
            "(?is).*['\\\"]connector['\\\"]\\s*=\\s*['\\\"]kafka['\\\"].*");
    private static final Pattern SENSITIVE=Pattern.compile(
            "(?is).*(password|secret|access[._-]?key|token|credential|sasl\\.jaas\\.config).*");

    private FlinkSqlGuard() {}

    public static void validate(String namespace,String table,String sourceDdl,String insertSql) {
        if(namespace==null || !namespace.matches("[A-Za-z_][A-Za-z0-9_]*") ||
           table==null || !table.matches("[A-Za-z_][A-Za-z0-9_]*"))
            throw new IllegalArgumentException("Invalid Iceberg target identifier");
        if(sourceDdl==null || insertSql==null || sourceDdl.isBlank() || insertSql.isBlank() ||
           sourceDdl.length()>20000 || insertSql.length()>20000)
            throw new IllegalArgumentException("Flink source DDL and INSERT SQL are required and limited to 20 KB");
        if(sourceDdl.contains(";") || insertSql.contains(";") ||
           sourceDdl.contains("--") || insertSql.contains("--") ||
           sourceDdl.contains("/*") || insertSql.contains("/*"))
            throw new IllegalArgumentException("Flink SQL must be a single statement without comments or statement separators");
        if(!SOURCE_DDL.matcher(sourceDdl.trim()).matches() ||
           !KAFKA_CONNECTOR.matcher(sourceDdl).matches())
            throw new IllegalArgumentException("Flink source DDL must define one unqualified Kafka source table");
        String normalized=insertSql.replace("`","").trim().toLowerCase(Locale.ROOT);
        String target="sbase_iceberg."+namespace+"."+table;
        if(!normalized.matches("(?s)^insert\\s+into\\s+"+Pattern.quote(target)+"(?:\\s|\\().*"))
            throw new IllegalArgumentException("Flink INSERT must target this pipeline's exact Iceberg table");
        if(SENSITIVE.matcher(sourceDdl).matches() || SENSITIVE.matcher(insertSql).matches())
            throw new IllegalArgumentException("Credentials must not be embedded in Flink SQL; use cluster secret configuration");
    }
}
