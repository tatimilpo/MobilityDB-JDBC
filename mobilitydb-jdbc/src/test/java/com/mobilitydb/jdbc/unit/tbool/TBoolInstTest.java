package com.mobilitydb.jdbc.unit.tbool;

import com.mobilitydb.jdbc.tbool.TBoolInst;
import com.mobilitydb.jdbc.temporal.TemporalType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

class TBoolInstTest {
    @Test
    void testConstructor() throws SQLException {
        String value = "true@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);

        TBoolInst tBoolInst = new TBoolInst(value);
        TBoolInst other = new TBoolInst(true, otherDate);

        assertEquals(tBoolInst.getTemporalValue(), other.getTemporalValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "true@2019-09-08 06:04:32+02",
        "false@2019-09-08 06:04:32+02"
    })
    void testInstSetType(String value) throws SQLException {
        TBoolInst tBoolInst = new TBoolInst(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT, tBoolInst.getTemporalType());
    }

    @Test
    void testInvalidEmptyValue() {
        SQLException thrown = assertThrows(
            SQLException.class,
            () -> {
                TBoolInst tBoolInst = new TBoolInst("");
            }
        );
        assertTrue(thrown.getMessage().contains("Value cannot be empty."));
    }

    @Test
    void testGetValue() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("false@2019-09-08 06:04:32+02");
        assertEquals(false, tBoolInst.getValue());
    }

    @Test
    void testStartValue() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        assertEquals(true, tBoolInst.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("false@2019-09-08 06:04:32+02");
        assertEquals(false, tBoolInst.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        assertEquals(true, tBoolInst.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("false@2019-09-08 06:04:32+02");
        assertEquals(false, tBoolInst.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TBoolInst tBoolInst = new TBoolInst("false@2019-09-08 06:10:32+02");
        assertNull(tBoolInst.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TBoolInst tBoolInst = new TBoolInst("false@2019-09-08 06:04:32+02");
        assertEquals(tBoolInst.getValue(), tBoolInst.valueAtTimestamp(timestamp));
    }

    @Test
    void testGetTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TBoolInst tBoolInst = new TBoolInst("false@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tBoolInst.getTimestamp());
    }
}

