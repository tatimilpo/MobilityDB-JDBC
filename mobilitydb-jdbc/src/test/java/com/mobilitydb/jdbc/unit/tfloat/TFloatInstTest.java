package com.mobilitydb.jdbc.unit.tfloat;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tfloat.TFloatInst;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
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

class TFloatInstTest {
    @Test
    void testConstructor() throws SQLException {
        String value = "2.5@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);

        TFloatInst tFloatInst = new TFloatInst(value);
        TFloatInst other = new TFloatInst(2.5f, otherDate);

        assertEquals(tFloatInst.getTemporalValue(), other.getTemporalValue());
        assertEquals(other, tFloatInst);
    }

    @Test
    void testEquals() throws SQLException {
        String value = "10.67@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        String otherValue = "12.77@2019-09-09 06:04:32+02";

        TFloatInst first = new TFloatInst(value);
        TFloatInst second = new TFloatInst(10.67f, otherDate);
        TFloatInst third = new TFloatInst(otherValue);

        assertEquals(first, second);
        assertNotEquals(first, third);
        assertNotEquals(second, third);
        assertNotEquals(first, new Object());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "0.8@2019-09-08 06:04:32+02",
            "25.89@2019-09-08 06:04:32+02"
    })
    void testInstSetType(String value) throws SQLException {
        TFloatInst tFloatInst = new TFloatInst(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT, tFloatInst.getTemporalType());
    }

    @Test
    void testInvalidEmptyValue() {
        SQLException thrown = assertThrows(
            SQLException.class,
            () -> {
                TFloatInst tFloatInst = new TFloatInst("");
            }
        );
        assertTrue(thrown.getMessage().contains("Value cannot be empty."));
    }

    @Test
    void testBuildValue() throws SQLException {
        String value = "1.8@2019-09-08 06:04:32";
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        value = value + format.format(tz);
        TFloatInst tFloatInst = new TFloatInst(value);
        String newValue = tFloatInst.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        String value = "25.74@2019-09-08 06:04:32+02";
        TFloatInst tFloatInst = new TFloatInst(value);
        List<Float> values = tFloatInst.getValues();
        assertEquals(1, values.size());
        assertEquals(25.74f, values.get(0));
    }

    @Test
    void testGetValue() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("36.2@2019-09-08 06:04:32+02");
        assertEquals(36.2f, tFloatInst.getValue());
    }

    @Test
    void testStartValue() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("5.0@2019-09-08 06:04:32+02");
        assertEquals(5.0f, tFloatInst.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("89.6@2019-09-08 06:04:32+02");
        assertEquals(89.6f, tFloatInst.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("2.1@2019-09-08 06:04:32+02");
        assertEquals(2.1f, tFloatInst.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:04:32+02");
        assertEquals(84.12f, tFloatInst.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        assertNull(tFloatInst.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:04:32+02");
        assertEquals(tFloatInst.getValue(), tFloatInst.valueAtTimestamp(timestamp));
    }

    @Test
    void testGetTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tFloatInst.getTimestamp());
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        assertEquals(1, tFloatInst.numTimestamps());
    }

    @Test
    void testTimestamps() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 10, 32, 0, tz);
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        assertEquals(1, tFloatInst.timestamps().length);
        assertEquals(expectedDate, tFloatInst.timestamps()[0]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 10, 32, 0, tz);
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        assertEquals(expectedDate, tFloatInst.timestampN(0));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tFloatInst.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 10, 32, 0, tz);
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        assertEquals(expectedDate, tFloatInst.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 10, 32, 0, tz);
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        assertEquals(expectedDate, tFloatInst.startTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2019,9, 8,
                6, 10, 32, 0, tz);
        Period period = new Period(date,date,true,true);
        assertEquals(period, tFloatInst.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2019,9, 8,
                6, 10, 32, 0, tz);
        Period period = new Period(date,date,true,true);
        PeriodSet periodSet = new PeriodSet(period);
        assertEquals(periodSet, tFloatInst.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        assertEquals(1, tFloatInst.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        assertEquals(tFloatInst, tFloatInst.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        assertEquals(tFloatInst, tFloatInst.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        assertEquals(tFloatInst, tFloatInst.instantN(0));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tFloatInst.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        ArrayList<TFloatInst> list = new ArrayList<>();
        list.add(tFloatInst);
        assertEquals(1, list.size());
        assertEquals(list, tFloatInst.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        assertEquals(Duration.ZERO, tFloatInst.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        assertEquals(Duration.ZERO, tFloatInst.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        TFloatInst otherTFloatInst = new TFloatInst("84.12@2019-09-10 06:10:32+02");
        tFloatInst.shift(Duration.ofDays(2));
        assertEquals(otherTFloatInst, tFloatInst);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:04:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        assertTrue(tFloatInst.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tFloatInst.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2001-01-08 06:10:32+02");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tFloatInst.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tFloatInst.intersectsPeriod(period));
    }

    @Test
    void testIntersectsPeriodNull() throws SQLException {
        TFloatInst tFloatInst = new TFloatInst("84.12@2019-09-08 06:10:32+02");
        assertFalse(tFloatInst.intersectsPeriod(null));
    }
}
