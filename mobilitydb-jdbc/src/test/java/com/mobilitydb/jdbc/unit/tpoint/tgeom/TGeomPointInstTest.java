package com.mobilitydb.jdbc.unit.tpoint.tgeom;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointInst;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

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
}
