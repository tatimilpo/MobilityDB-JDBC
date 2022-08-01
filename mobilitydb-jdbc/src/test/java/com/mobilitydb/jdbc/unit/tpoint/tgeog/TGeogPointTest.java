package com.mobilitydb.jdbc.unit.tpoint.tgeog;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tpoint.tgeog.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TGeogPointTest {
    @ParameterizedTest
    @ValueSource(strings = {
            "Point(0 0)@2019-09-08 06:04:32+02",
            "SRID=4326;Point(0 0)@2019-09-08 06:04:32+02"
    })
    void testConstructor_tGeogPointInst(String value) throws SQLException {
        TGeogPoint tGeogPoint = new TGeogPoint(value);
        assertEquals(TemporalType.TEMPORAL_INSTANT, tGeogPoint.getTemporalType());
        assertTrue(tGeogPoint.getTemporal() instanceof TGeogPointInst);
        TGeogPoint another = new TGeogPoint(tGeogPoint.getTemporal());
        assertEquals(tGeogPoint, another);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{Point(0 0)@2001-01-01 08:00:00+02, Point(2 2)@2001-01-03 08:00:00+02}",
            "SRID=4326;{Point(0 0)@2001-01-01 08:00:00+02, Point(2 2)@2001-01-03 08:00:00+02}"
    })
    void testConstructor_tGeogPointInstSet(String value) throws SQLException {
        TGeogPoint tGeogPoint = new TGeogPoint(value);
        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tGeogPoint.getTemporalType());
        assertTrue(tGeogPoint.getTemporal() instanceof TGeogPointInstSet);
        TGeogPoint another = new TGeogPoint(tGeogPoint.getTemporal());
        assertEquals(tGeogPoint, another);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "[Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02)",
            "SRID=4326;[Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02)"
    })
    void testConstructor_tGeogPointSeq(String value) throws SQLException {
        TGeogPoint tGeogPoint = new TGeogPoint(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE, tGeogPoint.getTemporalType());
        assertTrue(tGeogPoint.getTemporal() instanceof TGeogPointSeq);
        TGeogPoint another = new TGeogPoint(tGeogPoint.getTemporal());
        assertEquals(tGeogPoint, another);
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
    void testConstructor_tGeogPointSeqSet(String value) throws SQLException {
        TGeogPoint tGeogPoint = new TGeogPoint(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE_SET, tGeogPoint.getTemporalType());
        assertTrue(tGeogPoint.getTemporal() instanceof TGeogPointSeqSet);
        TGeogPoint another = new TGeogPoint(tGeogPoint.getTemporal());
        assertEquals(tGeogPoint, another);
    }
}
