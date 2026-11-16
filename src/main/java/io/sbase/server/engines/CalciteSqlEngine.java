package io.sbase.server.engines;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.DataContext;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

/**
 * Embedded Calcite SQL runtime for workflow-local JSON datasets.
 * Tables are supplied as {"tableName":[{"column":value}, ...]}.
 */
public final class CalciteSqlEngine {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final TypeReference<LinkedHashMap<String, List<LinkedHashMap<String, Object>>>> TABLES =
            new TypeReference<>() {};

    public String executeJson(String sql, String tablesJson) {
        Objects.requireNonNull(sql, "SQL query is required");
        try {
            Class.forName("org.apache.calcite.jdbc.Driver");
            Map<String, List<LinkedHashMap<String, Object>>> input =
                    JSON.readValue(tablesJson == null ? "{}" : tablesJson, TABLES);
            Properties properties = new Properties();
            try (Connection connection = DriverManager.getConnection("jdbc:calcite:", properties)) {
                CalciteConnection calcite = connection.unwrap(CalciteConnection.class);
                SchemaPlus root = calcite.getRootSchema();
                SchemaPlus workflow = root.add("WORKFLOW", new AbstractSchema());
                calcite.setSchema("WORKFLOW");
                for (Map.Entry<String, List<LinkedHashMap<String, Object>>> entry : input.entrySet()) {
                    validateIdentifier(entry.getKey(), "table");
                    if (entry.getValue() == null || entry.getValue().isEmpty()) {
                        throw new IllegalArgumentException("Calcite input table must contain at least one row: " + entry.getKey());
                    }
                    workflow.add(entry.getKey(), new JsonRowsTable(entry.getValue()));
                }
                List<LinkedHashMap<String, Object>> result = new ArrayList<>();
                try (Statement statement = connection.createStatement();
                     ResultSet rows = statement.executeQuery(sql)) {
                    ResultSetMetaData metadata = rows.getMetaData();
                    while (rows.next()) {
                        LinkedHashMap<String, Object> row = new LinkedHashMap<>();
                        for (int column = 1; column <= metadata.getColumnCount(); column++) {
                            Object value = rows.getObject(column);
                            if (value instanceof java.sql.Date || value instanceof java.sql.Timestamp
                                    || value instanceof java.sql.Time) value = value.toString();
                            row.put(metadata.getColumnLabel(column), value);
                        }
                        result.add(row);
                    }
                }
                return JSON.writeValueAsString(result);
            }
        } catch (SQLException | java.io.IOException | ClassNotFoundException error) {
            throw new IllegalArgumentException("Calcite SQL execution failed: " + error.getMessage(), error);
        }
    }

    private static void validateIdentifier(String identifier, String kind) {
        if (identifier == null || !identifier.matches("[A-Za-z_][A-Za-z0-9_]*")) {
            throw new IllegalArgumentException("Invalid Calcite " + kind + " identifier: " + identifier);
        }
    }

    private static final class JsonRowsTable extends AbstractTable implements ScannableTable {
        private final List<LinkedHashMap<String, Object>> rows;
        private final List<String> columns;
        private final Map<String, SqlTypeName> types;

        private JsonRowsTable(List<LinkedHashMap<String, Object>> rows) {
            this.rows = List.copyOf(rows);
            LinkedHashSet<String> names = new LinkedHashSet<>();
            rows.forEach(row -> names.addAll(row.keySet()));
            if (names.isEmpty()) throw new IllegalArgumentException("Calcite input rows need at least one column");
            names.forEach(name -> validateIdentifier(name, "column"));
            this.columns = List.copyOf(names);
            this.types = new LinkedHashMap<>();
            for (String column : columns) {
                Object sample = rows.stream().map(row -> row.get(column)).filter(Objects::nonNull).findFirst().orElse(null);
                types.put(column, sqlType(sample));
            }
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory factory) {
            RelDataTypeFactory.Builder builder = factory.builder();
            for (String column : columns) builder.add(column, types.get(column)).nullable(true);
            return builder.build();
        }

        @Override
        public Enumerable<Object[]> scan(DataContext root) {
            List<Object[]> values = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) {
                Object[] valuesForRow = new Object[columns.size()];
                for (int index = 0; index < columns.size(); index++) {
                    valuesForRow[index] = normalize(row.get(columns.get(index)), types.get(columns.get(index)));
                }
                values.add(valuesForRow);
            }
            return Linq4j.asEnumerable(values);
        }

        private static SqlTypeName sqlType(Object value) {
            if (value instanceof Boolean) return SqlTypeName.BOOLEAN;
            if (value instanceof Byte || value instanceof Short || value instanceof Integer) return SqlTypeName.INTEGER;
            if (value instanceof Long) return SqlTypeName.BIGINT;
            if (value instanceof Number) return SqlTypeName.DOUBLE;
            return SqlTypeName.VARCHAR;
        }

        private static Object normalize(Object value, SqlTypeName type) {
            if (value == null) return null;
            if (type == SqlTypeName.INTEGER) return ((Number) value).intValue();
            if (type == SqlTypeName.BIGINT) return ((Number) value).longValue();
            if (type == SqlTypeName.DOUBLE) return ((Number) value).doubleValue();
            if (type == SqlTypeName.VARCHAR) return value.toString();
            if (type == SqlTypeName.BOOLEAN) return value;
            if (value instanceof BigDecimal) return value;
            return value;
        }
    }
}
