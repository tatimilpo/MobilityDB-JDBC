package com.mobilitydb.jdbc.unit.tbool;

import com.mobilitydb.jdbc.tbool.TBoolInst;
import com.mobilitydb.jdbc.temporal.TemporalType;

import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;

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
    void testNullTimestamp() {
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> {
                    TBoolInst tBoolInst = new TBoolInst(true, null);
                }
        );
        assertTrue(thrown.getMessage().contains("Timestamp cannot be null."));
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

    @Test
    void testNumTimestamps() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        assertEquals(1, tBoolInst.numTimestamps());
    }

    @Test
    void testTimestamps() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        assertEquals(1, tBoolInst.timestamps().length);
        assertEquals(expectedDate, tBoolInst.timestamps()[0]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tBoolInst.timestampN(0));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tBoolInst.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tBoolInst.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TBoolInst tBoolInst = new TBoolInst("false@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tBoolInst.startTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("false@2019-09-08 06:04:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        Period period = new Period(date,date,true,true);
        assertEquals(period, tBoolInst.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("false@2019-09-08 06:04:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        Period period = new Period(date,date,true,true);
        PeriodSet periodSet = new PeriodSet(period);
        assertEquals(periodSet, tBoolInst.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        assertEquals(1, tBoolInst.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        assertEquals(tBoolInst, tBoolInst.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        assertEquals(tBoolInst, tBoolInst.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        assertEquals(tBoolInst, tBoolInst.instantN(0));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tBoolInst.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        ArrayList<TBoolInst> list = new ArrayList<>();
        list.add(tBoolInst);
        assertEquals(1, list.size());
        assertEquals(list, tBoolInst.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        assertEquals(Duration.ZERO, tBoolInst.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        assertEquals(Duration.ZERO, tBoolInst.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        TBoolInst otherTBoolInst = new TBoolInst("true@2019-09-10 06:04:32+02");
        tBoolInst.shift(Duration.ofDays(2));
        assertEquals(otherTBoolInst, tBoolInst);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        assertTrue(tBoolInst.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tBoolInst.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2001-01-08 06:04:32+02");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tBoolInst.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tBoolInst.intersectsPeriod(period));
    }

    @Test
    void testIntersectsPeriodNull() throws SQLException {
        TBoolInst tBoolInst = new TBoolInst("true@2019-09-08 06:04:32+02");
        assertFalse(tBoolInst.intersectsPeriod(null));
    }
}

