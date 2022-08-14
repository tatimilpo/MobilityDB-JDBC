package com.mobilitydb.jdbc.core;

import java.time.OffsetDateTime;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;

/**
 * Helper to format dateTime values
 */
public final class DateTimeFormatHelper {
    private static final DateTimeFormatter offsetFormatter = (new DateTimeFormatterBuilder())
            .appendOffset("+HH", "+00")
            .toFormatter();

    private static final DateTimeFormatter formatter = (new DateTimeFormatterBuilder())
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral(" ")
            .append(DateTimeFormatter.ISO_LOCAL_TIME)
            .appendOptional(offsetFormatter)
            .parseDefaulting(ChronoField.OFFSET_SECONDS, 0)
            .toFormatter();

    private DateTimeFormatHelper() {}

    /**
     * Formats a string into an OffsetDateTime
     * @param value String
     * @return OffsetDateTime
     */
    public static OffsetDateTime getDateTimeFormat(String value) {
        return OffsetDateTime.parse(value.trim(), formatter);
    }

    /**
     * Formats an OffsetDateTime into a string
     * @param value OffsetDateTime
     * @return String
     */
    public static String getStringFormat(OffsetDateTime value) {
        return formatter.format(value);
    }
}
