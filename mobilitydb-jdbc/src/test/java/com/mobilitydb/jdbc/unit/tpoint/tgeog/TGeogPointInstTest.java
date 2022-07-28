package com.mobilitydb.jdbc.unit.tpoint.tgeog;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointInst;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

class TGeogPointInstTest {
    @Test
    void testConstructor() throws SQLException {
        String value = "Point(0 0)@2017-01-01 08:00:05+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2017,1, 1,
                8, 0, 5, 0, tz);

        TGeogPointInst tGeogPointInst = new TGeogPointInst(value);
        TGeogPointInst other = new TGeogPointInst(new Point(0 ,0), otherDate);

        assertEquals(other.getTemporalValue(), tGeogPointInst.getTemporalValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SRID=4326;Point(10.0 10.0)@2021-04-08 05:04:45+01",
            "Point(1 0)@2017-01-01 08:00:05+02",
            "SRID=4326;010100000000000000000000000000000000000000@2021-04-08 05:04:45+01",
            "010100000000000000000000000000000000000000@2021-04-08 05:04:45+01"
    })
    void testInstSetType(String value) throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT, tGeogPointInst.getTemporalType());
    }

    @Test
    void testInvalidEmptyValue() {
        SQLException thrown = assertThrows(
            SQLException.class,
            () -> {
                TGeogPointInst tGeogPointInst = new TGeogPointInst("");
            }
        );
        assertTrue(thrown.getMessage().contains("Value cannot be empty."));
    }

    @Test
    void testGetValue() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        Point p = new Point(0,0);
        p.setSrid(4326);
        assertEquals(p , tGeogPointInst.getValue());
    }

    @Test
    void testStartValue() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        Point p = new Point(0,0);
        p.setSrid(4326);
        assertEquals(p, tGeogPointInst.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        Point p = new Point(0,0);
        p.setSrid(4326);
        assertEquals(p, tGeogPointInst.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        Point p = new Point(0,0);
        p.setSrid(4326);
        assertEquals(p, tGeogPointInst.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        Point p = new Point(0,0);
        p.setSrid(4326);
        assertEquals(p, tGeogPointInst.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:10:32+02");
        assertNull(tGeogPointInst.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        assertEquals(tGeogPointInst.getValue(), tGeogPointInst.valueAtTimestamp(timestamp));
    }

    @Test
    void testGetTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tGeogPointInst.getTimestamp());
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        assertEquals(1, tGeogPointInst.numTimestamps());
    }

    @Test
    void testTimestamps() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        assertEquals(1, tGeogPointInst.timestamps().length);
        assertEquals(expectedDate, tGeogPointInst.timestamps()[0]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tGeogPointInst.timestampN(0));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeogPointInst.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tGeogPointInst.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tGeogPointInst.startTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        Period period = new Period(date,date,true,true);
        assertEquals(period, tGeogPointInst.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        Period period = new Period(date,date,true,true);
        PeriodSet periodSet = new PeriodSet(period);
        assertEquals(periodSet, tGeogPointInst.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        assertEquals(1, tGeogPointInst.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        assertEquals(tGeogPointInst, tGeogPointInst.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        assertEquals(tGeogPointInst, tGeogPointInst.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        assertEquals(tGeogPointInst, tGeogPointInst.instantN(0));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeogPointInst.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        ArrayList<TGeogPointInst> list = new ArrayList<>();
        list.add(tGeogPointInst);
        assertEquals(1, list.size());
        assertEquals(list, tGeogPointInst.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        assertEquals(Duration.ZERO, tGeogPointInst.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        assertEquals(Duration.ZERO, tGeogPointInst.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 08:00:05+02");
        TGeogPointInst otherTGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-10 08:00:05+02");
        tGeogPointInst.shift(Duration.ofDays(2));
        assertEquals(otherTGeogPointInst, tGeogPointInst);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 08:00:05+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2019,9, 8,
                8, 0, 5, 0, tz);
        assertTrue(tGeogPointInst.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 08:00:05+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tGeogPointInst.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2001-01-08 08:00:05+02");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tGeogPointInst.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 08:00:05+02");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tGeogPointInst.intersectsPeriod(period));
    }

    @Test
    void testIntersectsPeriodNull() throws SQLException {
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 0)@2019-09-08 08:00:05+02");
        assertFalse(tGeogPointInst.intersectsPeriod(null));
    }
}
