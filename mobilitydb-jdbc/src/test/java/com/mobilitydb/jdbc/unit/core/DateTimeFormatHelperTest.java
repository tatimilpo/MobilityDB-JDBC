package com.mobilitydb.jdbc.unit.core;

import com.mobilitydb.jdbc.core.DateTimeFormatHelper;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DateTimeFormatHelperTest {
    @Test
    void getDateTimeFormat_test() {
        OffsetDateTime date = DateTimeFormatHelper.getDateTimeFormat("2018-01-04 00:28:47+00");
        OffsetDateTime dateWithoutOffset = DateTimeFormatHelper.getDateTimeFormat("2018-01-04 00:28:47");
        assertEquals(date, dateWithoutOffset);
        assertEquals(DateTimeFormatHelper.getStringFormat(date), DateTimeFormatHelper.getStringFormat(dateWithoutOffset));
    }
}
