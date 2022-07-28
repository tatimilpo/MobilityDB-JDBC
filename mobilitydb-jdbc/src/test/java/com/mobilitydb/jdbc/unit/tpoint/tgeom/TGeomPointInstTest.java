package com.mobilitydb.jdbc.unit.tpoint.tgeom;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointInst;
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

class TGeomPointInstTest {
    @Test
    void testConstructor() throws SQLException {
        String value = "Point(0 0)@2017-01-01 08:00:05+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2017,1, 1,
                8, 0, 5, 0, tz);

        TGeomPointInst tGeomPointInst = new TGeomPointInst(value);
        TGeomPointInst other = new TGeomPointInst(new Point(0 ,0), otherDate);

        assertEquals(other.getTemporalValue(), tGeomPointInst.getTemporalValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "SRID=4326;Point(10.0 10.0)@2021-04-08 05:04:45+01",
            "Point(1 0)@2017-01-01 08:00:05+02",
            "SRID=4326;010100000000000000000000000000000000000000@2021-04-08 05:04:45+01",
            "010100000000000000000000000000000000000000@2021-04-08 05:04:45+01"
    })
    void testInstSetType(String value) throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst(value);
        assertEquals( TemporalType.TEMPORAL_INSTANT, tGeomPointInst.getTemporalType());
    }

    @Test
    void testInvalidEmptyValue() {
        SQLException thrown = assertThrows(
            SQLException.class,
            () -> {
                TGeomPointInst tGeomPointInst = new TGeomPointInst("");
            }
        );
        assertTrue(thrown.getMessage().contains("Value cannot be empty."));
    }

    @Test
    void testGetValue() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        Point p = new Point(0,0);
        assertEquals(p, tGeomPointInst.getValue());
    }

    @Test
    void testStartValue() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        Point p = new Point(0,0);
        assertEquals(p, tGeomPointInst.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        Point p = new Point(0,0);
        assertEquals(p, tGeomPointInst.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        Point p = new Point(0,0);
        assertEquals(p, tGeomPointInst.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        Point p = new Point(0,0);
        assertEquals(p, tGeomPointInst.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2019-09-08 06:10:32+02");
        assertNull(tGeomPointInst.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        assertEquals(tGeomPointInst.getValue(), tGeomPointInst.valueAtTimestamp(timestamp));
    }

    @Test
    void testGetTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tGeomPointInst.getTimestamp());
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        assertEquals(1, tGeomPointInst.numTimestamps());
    }

    @Test
    void testTimestamps() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2017,1, 1,
                8, 0, 5, 0, tz);
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        assertEquals(1, tGeomPointInst.timestamps().length);
        assertEquals(expectedDate, tGeomPointInst.timestamps()[0]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2017,1, 1,
                8, 0, 5, 0, tz);
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        assertEquals(expectedDate, tGeomPointInst.timestampN(0));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeomPointInst.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2017,1, 1,
                8, 0, 5, 0, tz);
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        assertEquals(expectedDate, tGeomPointInst.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2017,1, 1,
                8, 0, 5, 0, tz);
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        assertEquals(expectedDate, tGeomPointInst.startTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2017,1, 1,
                8, 0, 5, 0, tz);
        Period period = new Period(date,date,true,true);
        assertEquals(period, tGeomPointInst.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2017,1, 1,
                8, 0, 5, 0, tz);
        Period period = new Period(date,date,true,true);
        PeriodSet periodSet = new PeriodSet(period);
        assertEquals(periodSet, tGeomPointInst.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        assertEquals(1, tGeomPointInst.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        assertEquals(tGeomPointInst, tGeomPointInst.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        assertEquals(tGeomPointInst, tGeomPointInst.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        assertEquals(tGeomPointInst, tGeomPointInst.instantN(0));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeomPointInst.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        ArrayList<TGeomPointInst> list = new ArrayList<>();
        list.add(tGeomPointInst);
        assertEquals(1, list.size());
        assertEquals(list, tGeomPointInst.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        assertEquals(Duration.ZERO, tGeomPointInst.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        assertEquals(Duration.ZERO, tGeomPointInst.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        TGeomPointInst otherTGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-03 08:00:05+02");
        tGeomPointInst.shift(Duration.ofDays(2));
        assertEquals(otherTGeomPointInst, tGeomPointInst);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2019-09-01 08:00:05+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2019,9, 1,
                8, 0, 5, 0, tz);
        assertTrue(tGeomPointInst.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tGeomPointInst.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2001-01-03 08:00:05+02");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tGeomPointInst.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tGeomPointInst.intersectsPeriod(period));
    }

    @Test
    void testIntersectsPeriodNull() throws SQLException {
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2017-01-01 08:00:05+02");
        assertFalse(tGeomPointInst.intersectsPeriod(null));
    }
}
