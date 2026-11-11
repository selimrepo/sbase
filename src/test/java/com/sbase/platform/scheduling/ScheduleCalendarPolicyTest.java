package com.sbase.platform.scheduling;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import java.time.LocalDate;
import static org.junit.jupiter.api.Assertions.*;

class ScheduleCalendarPolicyTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void blocksConfiguredDatesAndWeekdays() {
        assertTrue(ScheduleCalendarPolicy.blocks(mapper,
            "{\"skipDates\":[\"2026-12-25\"]}", LocalDate.parse("2026-12-25")));
        assertTrue(ScheduleCalendarPolicy.blocks(mapper,
            "{\"skipWeekdays\":[\"SUNDAY\"]}", LocalDate.parse("2026-11-15")));
    }

    @Test
    void includeDatesActsAsAnAllowListWhenConfigured() {
        String calendar = "{\"includeDates\":[\"2026-11-11\",\"2026-11-12\"]}";
        assertFalse(ScheduleCalendarPolicy.blocks(mapper, calendar, LocalDate.parse("2026-11-11")));
        assertTrue(ScheduleCalendarPolicy.blocks(mapper, calendar, LocalDate.parse("2026-11-13")));
    }

    @Test
    void rejectsMalformedCalendarConfiguration() {
        assertThrows(IllegalArgumentException.class,
            () -> ScheduleCalendarPolicy.blocks(mapper, "{not-json", LocalDate.parse("2026-11-11")));
    }
}
