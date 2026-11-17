package io.sbase.server.engines;

/**
 * Normalizes identifiers that are reserved by Calcite but valid as ordinary
 * PostgreSQL/Spark column names before Calcite-only parsing. The original SQL
 * is always used for source execution and Spark execution.
 *
 * This is a lexical pass, not a regex replacement: quoted identifiers, string
 * literals, comments and PostgreSQL dollar-quoted strings are copied verbatim.
 */
public final class CalciteSqlCompatibility {
    private CalciteSqlCompatibility() {}

    public static String forParsing(String sql) {
        if (sql == null || sql.isEmpty()) return sql;
        StringBuilder out = new StringBuilder(sql.length() + 16);
        int i = 0;
        while (i < sql.length()) {
            char c = sql.charAt(i);
            if (c == '\'' || c == '"' || c == '`') {
                i = copyQuoted(sql, i, c, out);
            } else if (c == '[') {
                i = copyBracketIdentifier(sql, i, out);
            } else if (c == '-' && i + 1 < sql.length() && sql.charAt(i + 1) == '-') {
                int end = sql.indexOf('\n', i);
                if (end < 0) end = sql.length();
                out.append(sql, i, end); i = end;
            } else if (c == '/' && i + 1 < sql.length() && sql.charAt(i + 1) == '*') {
                int end = sql.indexOf("*/", i + 2);
                end = end < 0 ? sql.length() : end + 2;
                out.append(sql, i, end); i = end;
            } else if (c == '$' && dollarTagEnd(sql, i) >= 0) {
                int tagEnd = dollarTagEnd(sql, i);
                String tag = sql.substring(i, tagEnd);
                int close = sql.indexOf(tag, tagEnd);
                int end = close < 0 ? sql.length() : close + tag.length();
                out.append(sql, i, end); i = end;
            } else if (Character.isLetter(c) || c == '_') {
                int end = i + 1;
                while (end < sql.length() && (Character.isLetterOrDigit(sql.charAt(end)) || sql.charAt(end) == '_' || sql.charAt(end) == '$')) end++;
                String word = sql.substring(i, end);
                if (word.equalsIgnoreCase("value") && !followedByCall(sql, end)) out.append('"').append(word).append('"');
                else out.append(word);
                i = end;
            } else {
                out.append(c); i++;
            }
        }
        return out.toString();
    }

    private static boolean followedByCall(String sql, int index) {
        while (index < sql.length() && Character.isWhitespace(sql.charAt(index))) index++;
        return index < sql.length() && sql.charAt(index) == '(';
    }

    private static int copyQuoted(String sql, int start, char quote, StringBuilder out) {
        int i = start + 1;
        while (i < sql.length()) {
            if (sql.charAt(i) == quote) {
                if (i + 1 < sql.length() && sql.charAt(i + 1) == quote) { i += 2; continue; }
                i++; break;
            }
            if (sql.charAt(i) == '\\' && quote == '\'' && i + 1 < sql.length()) i += 2;
            else i++;
        }
        out.append(sql, start, i);
        return i;
    }

    private static int copyBracketIdentifier(String sql, int start, StringBuilder out) {
        int end = sql.indexOf(']', start + 1);
        end = end < 0 ? sql.length() : end + 1;
        out.append(sql, start, end);
        return end;
    }

    private static int dollarTagEnd(String sql, int start) {
        int i = start + 1;
        while (i < sql.length() && (Character.isLetterOrDigit(sql.charAt(i)) || sql.charAt(i) == '_')) i++;
        return i < sql.length() && sql.charAt(i) == '$' ? i + 1 : -1;
    }
}
