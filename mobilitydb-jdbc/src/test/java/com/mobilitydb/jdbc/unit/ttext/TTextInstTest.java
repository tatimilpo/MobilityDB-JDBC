package com.mobilitydb.jdbc.unit.ttext;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.ttext.TTextInst;
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

class TTextInstTest {
    @Test
    void testConstructor() throws SQLException {
        String value = "abccccd@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);

        TTextInst tTextInst = new TTextInst(value);
        TTextInst other = new TTextInst("abccccd", otherDate);

        assertEquals(other.getTemporalValue(), tTextInst.getTemporalValue());
        assertEquals(other, tTextInst);
    }

    @Test
    void testEquals() throws SQLException {
        String value = "abcd@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        String otherValue = "ijfi@2019-09-09 06:04:32+02";

        TTextInst first = new TTextInst(value);
        TTextInst second = new TTextInst("abcd", otherDate);
        TTextInst third = new TTextInst(otherValue);

        assertEquals(first, second);
        assertNotEquals(first, third);
        assertNotEquals(second, third);
        assertNotEquals(first, new Object());
    }


    @ParameterizedTest
    @ValueSource(strings = {
            "abcd@2019-09-08 06:04:32+02",
            "TEST@2019-09-08 06:04:32+02",
            "This is a Test@2019-09-08 06:04:32+02"
    })
    void testInstSetType(String value) throws SQLException {
        TTextInst tTextInst = new TTextInst(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT, tTextInst.getTemporalType());
    }

    @Test
    void testInvalidEmptyValue() {
        SQLException thrown = assertThrows(
            SQLException.class,
            () -> {
                TTextInst tTextInst = new TTextInst("");
            }
        );
        assertTrue(thrown.getMessage().contains("Value cannot be empty."));
    }

    @Test
    void testBuildValue() throws SQLException {
        String value = "sfcsff@2019-09-08 06:04:32";
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        value = value + format.format(tz);
        TTextInst tIntInst = new TTextInst(value);
        String newValue = tIntInst.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        String value = "Test@2019-09-08 06:04:32+02";
        TTextInst tTextInst = new TTextInst(value);
        List<String> values = tTextInst.getValues();
        assertEquals(1, values.size());
        assertEquals("Test", values.get(0));
    }

    @Test
    void testGetValue() throws SQLException {
        TTextInst tTextInst = new TTextInst("test@2019-09-08 06:04:32+02");
        assertEquals("test", tTextInst.getValue());
    }

    @Test
    void testStartValue() throws SQLException {
        TTextInst tTextInst = new TTextInst("test12@2019-09-08 06:04:32+02");
        assertEquals("test12", tTextInst.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TTextInst tTextInst = new TTextInst("test145@2019-09-08 06:04:32+02");
        assertEquals("test145", tTextInst.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TTextInst tTextInst = new TTextInst("abcd@2019-09-08 06:04:32+02");
        assertEquals("abcd", tTextInst.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TTextInst tTextInst = new TTextInst("this is a test@2019-09-08 06:04:32+02");
        assertEquals("this is a test", tTextInst.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TTextInst tTextInst = new TTextInst("what@2019-09-08 06:10:32+02");
        assertNull(tTextInst.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TTextInst tTextInst = new TTextInst("which@2019-09-08 06:04:32+02");
        assertEquals(tTextInst.getValue(), tTextInst.valueAtTimestamp(timestamp));
    }

    @Test
    void testGetTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tTextInst.getTimestamp());
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        assertEquals(1, tTextInst.numTimestamps());
    }

    @Test
    void testTimestamps() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        assertEquals(1, tTextInst.timestamps().length);
        assertEquals(expectedDate, tTextInst.timestamps()[0]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tTextInst.timestampN(0));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tTextInst.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tTextInst.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tTextInst.startTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        Period period = new Period(date,date,true,true);
        assertEquals(period, tTextInst.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        Period period = new Period(date,date,true,true);
        PeriodSet periodSet = new PeriodSet(period);
        assertEquals(periodSet, tTextInst.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        assertEquals(1, tTextInst.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        assertEquals(tTextInst, tTextInst.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        assertEquals(tTextInst, tTextInst.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        assertEquals(tTextInst, tTextInst.instantN(0));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tTextInst.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        ArrayList<TTextInst> list = new ArrayList<>();
        list.add(tTextInst);
        assertEquals(1, list.size());
        assertEquals(list, tTextInst.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        assertEquals(Duration.ZERO, tTextInst.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        assertEquals(Duration.ZERO, tTextInst.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        TTextInst otherTTextInst = new TTextInst("who@2019-09-10 06:04:32+02");
        tTextInst.shift(Duration.ofDays(2));
        assertEquals(otherTTextInst, tTextInst);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        assertTrue(tTextInst.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tTextInst.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2001-01-08 06:04:32+02");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tTextInst.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tTextInst.intersectsPeriod(period));
    }

    @Test
    void testIntersectsPeriodNull() throws SQLException {
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        assertFalse(tTextInst.intersectsPeriod(null));
    }

}
