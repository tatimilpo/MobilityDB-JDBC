package com.mobilitydb.jdbc.unit.tpoint.tgeom;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tpoint.tgeom.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TGeomPointTest {
    @ParameterizedTest
    @ValueSource(strings = {
            "Point(0 0)@2019-09-08 06:04:32+02",
            "SRID=4326;Point(0 0)@2019-09-08 06:04:32+02"
    })
    void testConstructorTGeomPointInst(String value ) throws SQLException {
        TGeomPoint tGeomPoint = new TGeomPoint(value);
        assertEquals(TemporalType.TEMPORAL_INSTANT, tGeomPoint.getTemporalType());
        assertTrue(tGeomPoint.getTemporal() instanceof TGeomPointInst);
        TGeomPoint another = new TGeomPoint(tGeomPoint.getTemporal());
        assertEquals(tGeomPoint, another);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{Point(0 0)@2001-01-01 08:00:00+02, Point(2 2)@2001-01-03 08:00:00+02}",
            "SRID=4326;{Point(0 0)@2001-01-01 08:00:00+02, Point(2 2)@2001-01-03 08:00:00+02}"
    })
    void testConstructorTGeomPointInstSet(String value) throws SQLException {
        TGeomPoint tGeomPoint = new TGeomPoint(value);
        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tGeomPoint.getTemporalType());
        assertTrue(tGeomPoint.getTemporal() instanceof TGeomPointInstSet);
        TGeomPoint another = new TGeomPoint(tGeomPoint.getTemporal());
        assertEquals(tGeomPoint, another);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "[Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02)",
            "SRID=4326;[Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02)"
    })
    void testConstructorTGeomPointSeq(String value) throws SQLException {
        TGeomPoint tGeomPoint = new TGeomPoint(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE, tGeomPoint.getTemporalType());
        assertTrue(tGeomPoint.getTemporal() instanceof TGeomPointSeq);
        TGeomPoint another = new TGeomPoint(tGeomPoint.getTemporal());
        assertEquals(tGeomPoint, another);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                    "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02]}",
            "SRID=4326;{[POINT(1 1)@2001-01-01 08:00:00+02, POINT(1 1)@2001-01-03 08:00:00+02), " +
                    "[POINT(1 1)@2001-01-04 08:00:00+02, POINT(1 1)@2001-01-05 08:00:00+02]}"
    })
    void testConstructorTGeomPointSeqSet(String value) throws SQLException {
        TGeomPoint tGeomPoint = new TGeomPoint(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE_SET, tGeomPoint.getTemporalType());
        assertTrue(tGeomPoint.getTemporal() instanceof TGeomPointSeqSet);
        TGeomPoint another = new TGeomPoint(tGeomPoint.getTemporal());
        assertEquals(tGeomPoint, another);
    }
}
