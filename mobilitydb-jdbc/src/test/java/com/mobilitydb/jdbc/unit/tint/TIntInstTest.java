package com.mobilitydb.jdbc.unit.tint;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.tint.TIntInst;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TIntInstTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "10@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);

        TIntInst tIntInst = new TIntInst(value);
        TIntInst other = new TIntInst(10, otherDate);

        assertEquals(other.getTemporalValue(), tIntInst.getTemporalValue());
        assertEquals(other, tIntInst);
    }

    @Test
    void testEquals() throws SQLException {
        String value = "10@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        String otherValue = "12@2019-09-09 06:04:32+02";

        TIntInst first = new TIntInst(value);
        TIntInst second = new TIntInst(10, otherDate);
        TIntInst third = new TIntInst(otherValue);

        assertEquals(first, second);
        assertNotEquals(first, third);
        assertNotEquals(second, third);
        assertNotEquals(first, new Object());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "254@2019-09-08 06:04:32+02",
            "659@2019-09-08 06:04:32+02"
    })
    void testInstSetType(String value) throws SQLException {
        TIntInst tIntInst = new TIntInst(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT, tIntInst.getTemporalType());
    }

    @Test
    void testInvalidEmptyValue() {
        SQLException thrown = assertThrows(
            SQLException.class,
            () -> {
                TIntInst tIntInst = new TIntInst("");
            }
        );
        assertTrue(thrown.getMessage().contains("Value cannot be empty."));
    }

    @Test
    void testBuildValue() throws SQLException {
        String value = "10@2019-09-08 06:04:32";
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        value = value + format.format(tz);
        TIntInst tIntInst = new TIntInst(value);
        String newValue = tIntInst.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        String value = "10@2019-09-08 06:04:32+02";
        TIntInst tIntInst = new TIntInst(value);
        List<Integer> values = tIntInst.getValues();
        assertEquals(1, values.size());
        assertEquals(10, values.get(0));
    }

    @Test
    void testGetValue() throws SQLException {
        TIntInst tIntInst = new TIntInst("8@2019-09-08 06:04:32+02");
        assertEquals(8, tIntInst.getValue());
    }

    @Test
    void testStartValue() throws SQLException {
        TIntInst tIntInst = new TIntInst("9@2019-09-08 06:04:32+02");
        assertEquals(9, tIntInst.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TIntInst tIntInst = new TIntInst("7@2019-09-08 06:04:32+02");
        assertEquals(7, tIntInst.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TIntInst tIntInst = new TIntInst("3@2019-09-08 06:04:32+02");
        assertEquals(3, tIntInst.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TIntInst tIntInst = new TIntInst("85@2019-09-08 06:04:32+02");
        assertEquals(85, tIntInst.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TIntInst tIntInst = new TIntInst("78@2019-09-08 06:10:32+02");
        assertNull(tIntInst.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TIntInst tIntInst = new TIntInst("63@2019-09-08 06:04:32+02");
        assertEquals(tIntInst.getValue(), tIntInst.valueAtTimestamp(timestamp));
    }

    @Test
    void testGetTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tIntInst.getTimestamp());
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        assertEquals(1, tIntInst.numTimestamps());
    }

    @Test
    void testTimestamps() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        assertEquals(1, tIntInst.timestamps().length);
        assertEquals(expectedDate, tIntInst.timestamps()[0]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tIntInst.timestampN(0));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tIntInst.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tIntInst.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tIntInst.startTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        Period period = new Period(date,date,true,true);
        assertEquals(period, tIntInst.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        Period period = new Period(date,date,true,true);
        PeriodSet periodSet = new PeriodSet(period);
        assertEquals(periodSet, tIntInst.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        assertEquals(1, tIntInst.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        assertEquals(tIntInst, tIntInst.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        assertEquals(tIntInst, tIntInst.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        assertEquals(tIntInst, tIntInst.instantN(0));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tIntInst.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        ArrayList<TIntInst> list = new ArrayList<>();
        list.add(tIntInst);
        assertEquals(1, list.size());
        assertEquals(list, tIntInst.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        assertEquals(Duration.ZERO, tIntInst.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        assertEquals(Duration.ZERO, tIntInst.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        TIntInst otherTIntInst = new TIntInst("89@2019-09-10 06:04:32+02");
        tIntInst.shift(Duration.ofDays(2));
        assertEquals(otherTIntInst, tIntInst);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        assertTrue(tIntInst.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tIntInst.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2001-01-08 06:04:32+02");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tIntInst.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tIntInst.intersectsPeriod(period));
    }

    @Test
    void testIntersectsPeriodNull() throws SQLException {
        TIntInst tIntInst = new TIntInst("89@2019-09-08 06:04:32+02");
        assertFalse(tIntInst.intersectsPeriod(null));
    }
}
