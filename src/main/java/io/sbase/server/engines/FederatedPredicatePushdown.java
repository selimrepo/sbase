package io.sbase.server.engines;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParser;
import java.util.*;

/**
 * Conservative SQL subset translator for source-side filtering. Unsupported expressions
 * are rejected so callers can safely fall back to a bounded full scan instead of
 * silently changing query semantics.
 */
public final class FederatedPredicatePushdown {
    private FederatedPredicatePushdown() {}

    public static Optional<String> translate(String sql, String source, String table, int limit) {
        try {
            SqlNode node = SqlParser.create(sql, SqlParser.config().withCaseSensitive(false)).parseQuery();
            if (!(node instanceof SqlSelect select)) return Optional.empty();
            if (!(select.getFrom() instanceof SqlIdentifier from) || from.names.size() != 1) return Optional.empty();
            if (select.getGroup() != null || select.getHaving() != null
                    || select.getWindowList() != null && select.getWindowList().size() > 0
                    || select.getQualify() != null
                    || select.getOrderList() != null && select.getOrderList().size() > 0) return Optional.empty();
            String where = select.getWhere() == null ? null : predicate(select.getWhere(), source);
            String dialect = source.toLowerCase(Locale.ROOT);
            if (dialect.equals("solr")) return Optional.of(where == null ? "*:*" : where);
            if (!dialect.equals("postgresql") && !dialect.equals("cassandra")) return Optional.empty();
            if (table == null || !table.matches("[A-Za-z_][A-Za-z0-9_.]*")) return Optional.empty();
            String projection = projection(select.getSelectList());
            StringBuilder query = new StringBuilder("SELECT ").append(projection).append(" FROM ").append(table);
            if (where != null) query.append(" WHERE ").append(where);
            query.append(" LIMIT ").append(Math.max(1, Math.min(limit, 5000)));
            return Optional.of(query.toString());
        } catch (Exception unsupported) {
            return Optional.empty();
        }
    }

    private static String projection(SqlNodeList list) {
        if (list == null || list.size() == 0) return "*";
        List<String> columns = new ArrayList<>();
        for (SqlNode node : list) {
            if (node instanceof SqlIdentifier id) {
                String name = id.names.get(id.names.size() - 1);
                if (!name.matches("[A-Za-z_][A-Za-z0-9_]*") && !name.equals("*"))
                    throw new IllegalArgumentException("Unsafe projection");
                columns.add(name.toLowerCase(Locale.ROOT));
            } else if (node instanceof SqlNodeList) {
                throw new IllegalArgumentException("Unsupported projection");
            } else {
                throw new IllegalArgumentException("Only direct column projections can be pushed down");
            }
        }
        return String.join(", ", columns);
    }

    private static String predicate(SqlNode node, String source) {
        if (!(node instanceof SqlCall call)) throw new IllegalArgumentException("Unsupported predicate");
        SqlKind kind = call.getKind();
        if (kind == SqlKind.AND || kind == SqlKind.OR) {
            if ("cassandra".equalsIgnoreCase(source) && kind == SqlKind.OR) throw new IllegalArgumentException("CQL does not support OR predicates");
            return "(" + predicate(call.operand(0), source) + (kind == SqlKind.AND ? " AND " : " OR ")
                    + predicate(call.operand(1), source) + ")";
        }
        if ("cassandra".equalsIgnoreCase(source) && kind == SqlKind.NOT_EQUALS) throw new IllegalArgumentException("CQL does not support != predicates");
        String operator = switch (kind) {
            case EQUALS -> "="; case NOT_EQUALS -> "!="; case GREATER_THAN -> ">";
            case GREATER_THAN_OR_EQUAL -> ">="; case LESS_THAN -> "<"; case LESS_THAN_OR_EQUAL -> "<=";
            default -> throw new IllegalArgumentException("Unsupported predicate operator");
        };
        SqlNode left = call.operand(0), right = call.operand(1);
        if (!(left instanceof SqlIdentifier id) || !(right instanceof SqlLiteral))
            throw new IllegalArgumentException("Predicate must compare a column to a literal");
        String field = id.names.get(id.names.size() - 1).toLowerCase(Locale.ROOT);
        if (!field.matches("[A-Za-z_][A-Za-z0-9_]*")) throw new IllegalArgumentException("Unsafe field");
        Object value = ((SqlLiteral) right).getValue();
        String literal = literal((SqlLiteral) right);
        if ("solr".equalsIgnoreCase(source)) {
            if (kind == SqlKind.EQUALS) return field + ":" + solrValue(value);
            String range = switch (kind) {
                case GREATER_THAN -> "{" + literal + " TO *]";
                case GREATER_THAN_OR_EQUAL -> "[" + literal + " TO *]";
                case LESS_THAN -> "[* TO " + literal + "}";
                case LESS_THAN_OR_EQUAL -> "[* TO " + literal + "]";
                default -> throw new IllegalArgumentException("Solr does not support this pushed operator");
            };
            return field + ":" + range;
        }
        return field + " " + operator + " " + literal;
    }

    private static String literal(SqlLiteral literal) {
        Object value = literal.getValue();
        if (value instanceof org.apache.calcite.util.NlsString nls) {
            return "'" + nls.getValue().replace("'", "''") + "'";
        }
        if (literal.getTypeName() == org.apache.calcite.sql.type.SqlTypeName.CHAR
                || literal.getTypeName() == org.apache.calcite.sql.type.SqlTypeName.VARCHAR) {
            String text = String.valueOf(value);
            return "'" + text.replace("'", "''") + "'";
        }
        String text = literal.toValue();
        if (text == null || !text.matches("-?[0-9]+(\\.[0-9]+)?|TRUE|FALSE|NULL"))
            throw new IllegalArgumentException("Unsupported literal");
        return text;
    }

    private static String solrValue(Object value) {
        String raw = value instanceof org.apache.calcite.util.NlsString nls ? nls.getValue() : String.valueOf(value);
        if (raw.matches("-?[0-9]+(\\.[0-9]+)?")) return raw;
        return "\"" + raw.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
