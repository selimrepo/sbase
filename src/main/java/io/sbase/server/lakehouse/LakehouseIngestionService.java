package io.sbase.server.lakehouse;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.util.Map;
import java.util.Objects;

/** Spark batch ingestion adapters. Secret values are resolved at runtime and never persisted. */
public final class LakehouseIngestionService {
    public interface SecretResolver { String resolve(String secretRef); }
    private final SparkSession spark;
    private final SecretResolver secrets;

    public LakehouseIngestionService(SparkSession spark, SecretResolver secrets) {
        this.spark=Objects.requireNonNull(spark);
        this.secrets=Objects.requireNonNull(secrets);
    }

    public Dataset<Row> readObjectStorage(String uri, String format, Map<String,String> options) {
        if(!uri.startsWith("s3://") && !uri.startsWith("s3a://")) throw new IllegalArgumentException("Only S3-compatible object URIs are accepted");
        String safeFormat=allow(format,"parquet","orc","json","csv","avro");
        var reader=spark.read().format(safeFormat);
        options.forEach((k,v)->reader.option(k,v));
        return reader.load(uri);
    }

    public Dataset<Row> readJdbc(String jdbcUrl, String tableOrQuery, String user, String secretRef,
                                 String driver, Map<String,String> options) {
        if(!jdbcUrl.startsWith("jdbc:")) throw new IllegalArgumentException("A JDBC URL is required");
        var reader=spark.read().format("jdbc").option("url",jdbcUrl)
            .option("dbtable",tableOrQuery).option("user",user)
            .option("password",secrets.resolve(secretRef));
        if(driver!=null && !driver.isBlank()) reader=reader.option("driver",driver);
        options.forEach((k,v)->reader.option(k,v));
        return reader.load();
    }

    public Dataset<Row> readKafkaBatch(String bootstrapServers, String topic, String startingOffsets,
                                       Map<String,String> options) {
        if(bootstrapServers==null || bootstrapServers.isBlank() || topic==null || topic.isBlank())
            throw new IllegalArgumentException("Kafka bootstrap servers and topic are required");
        var reader=spark.read().format("kafka").option("kafka.bootstrap.servers",bootstrapServers)
            .option("subscribe",topic).option("startingOffsets",startingOffsets==null?"earliest":startingOffsets);
        options.forEach((k,v)->reader.option(k,v));
        return reader.load();
    }

    public void appendIceberg(Dataset<Row> data, String catalog, String namespace, String table) {
        String identifier=identifier(catalog,namespace,table);
        data.writeTo(identifier).append();
    }

    public void createIcebergTable(Dataset<Row> data, String catalog, String namespace, String table,
                                   Map<String,String> properties) {
        String identifier=identifier(catalog,namespace,table);
        var writer=data.writeTo(identifier);
        properties.forEach(writer::tableProperty);
        writer.createOrReplace();
    }

    /** Iceberg's field-id based schema evolution is used; destructive changes are explicit. */
    public void addColumns(String identifier, String columnsDdl) {
        spark.sql("ALTER TABLE "+validatedIdentifier(identifier)+" ADD COLUMNS ("+validatedDdl(columnsDdl)+")");
    }

    public void renameColumn(String identifier, String oldName, String newName) {
        spark.sql("ALTER TABLE "+validatedIdentifier(identifier)+" RENAME COLUMN "+
            validatedIdentifier(oldName)+" TO "+validatedIdentifier(newName));
    }

    public void widenColumnType(String identifier, String column, String targetType) {
        String type=allow(targetType.toUpperCase(),"BIGINT","DOUBLE","DECIMAL(38,18)","STRING","TIMESTAMP","DATE");
        spark.sql("ALTER TABLE "+validatedIdentifier(identifier)+" ALTER COLUMN "+
            validatedIdentifier(column)+" TYPE "+type);
    }

    private static String identifier(String catalog,String namespace,String table) {
        return validatedIdentifier(catalog)+"."+validatedIdentifier(namespace)+"."+validatedIdentifier(table);
    }
    private static String validatedIdentifier(String value) {
        if(value==null || !value.matches("[A-Za-z_][A-Za-z0-9_]*")) throw new IllegalArgumentException("Unsafe SQL identifier: "+value);
        return "`"+value+"`";
    }
    private static String validatedDdl(String value) {
        if(value==null || value.isBlank() || value.contains(";") || value.contains("--") || value.contains("/*"))
            throw new IllegalArgumentException("Invalid column DDL");
        return value;
    }
    private static String allow(String value,String... allowed) {
        for(String candidate:allowed) if(candidate.equalsIgnoreCase(value)) return candidate.toLowerCase();
        throw new IllegalArgumentException("Unsupported format/type: "+value);
    }
}
