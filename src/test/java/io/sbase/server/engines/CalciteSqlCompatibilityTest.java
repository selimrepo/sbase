package io.sbase.server.engines;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CalciteSqlCompatibilityTest {
    @Test void quotesCalciteReservedColumnWithoutChangingStringsCommentsOrQuotedIdentifiers() {
        String sql = "SELECT value, 'value', \"value\", `value`, [value] -- value\\n"
                + "FROM records /* value */ WHERE value = 'it''s value' AND note = $$value$$";
        String normalized = CalciteSqlCompatibility.forParsing(sql);
        assertEquals("SELECT \"value\", 'value', \"value\", `value`, [value] -- value\\n"
                + "FROM records /* value */ WHERE \"value\" = 'it''s value' AND note = $$value$$", normalized);
    }

    @Test void leavesFunctionNamesAndOtherSqlWordsAlone() {
        String sql = "SELECT value_count(value), VALUE FROM metrics";
        assertEquals("SELECT value_count(value), \"VALUE\" FROM metrics",
                CalciteSqlCompatibility.forParsing(sql));
    }
}
