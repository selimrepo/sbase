package com.sbase.platform.scheduling;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDate;

final class ScheduleCalendarPolicy {
    private ScheduleCalendarPolicy() { }

    static boolean blocks(ObjectMapper mapper, String calendarJson, LocalDate date) {
        try {
            JsonNode calendar = mapper.readTree(calendarJson == null ? "{}" : calendarJson);
            JsonNode skipDates = calendar.path("skipDates");
            if (skipDates.isArray()) {
                for (JsonNode value : skipDates) if (date.toString().equals(value.asText())) return true;
            }
            JsonNode skipWeekdays = calendar.path("skipWeekdays");
            if (skipWeekdays.isArray()) {
                for (JsonNode value : skipWeekdays)
                    if (date.getDayOfWeek().name().equalsIgnoreCase(value.asText())) return true;
            }
            JsonNode includeDates = calendar.path("includeDates");
            if (includeDates.isArray() && !includeDates.isEmpty()) {
                for (JsonNode value : includeDates) if (date.toString().equals(value.asText())) return false;
                return true;
            }
            return false;
        } catch (Exception ex) {
            throw new IllegalArgumentException("Invalid scheduler calendar JSON", ex);
        }
    }
}
