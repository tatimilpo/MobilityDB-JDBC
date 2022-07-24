package com.mobilitydb.jdbc.unit.tpoint.tgeom;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointInst;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointSeq;
import org.junit.jupiter.api.Test;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TGeomPointSeqTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "[Point(1 1)@2001-01-01 08:00:00+02, Point(2 2)@2001-01-03 08:00:00+02)";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TGeomPointInst[] instants = new TGeomPointInst[]{
                new TGeomPointInst(new Point(1, 1), dateOne),
                new TGeomPointInst(new Point(2, 2), dateTwo)
        };
        String[] stringInstants = new String[]{
                "Point(1 1)@2001-01-01 08:00:00+02",
                "Point(2 2)@2001-01-03 08:00:00+02"
        };

        TGeomPointSeq firstTemporal = new TGeomPointSeq(value);
        TGeomPointSeq secondTemporal = new TGeomPointSeq(instants);
        TGeomPointSeq thirdTemporal = new TGeomPointSeq(stringInstants);
        TGeomPointSeq fourthTemporal = new TGeomPointSeq(instants, true, false);
        TGeomPointSeq fifthTemporal = new TGeomPointSeq(stringInstants, true, false);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
        assertEquals(firstTemporal, fourthTemporal);
        assertEquals(firstTemporal, fifthTemporal);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "[Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02)";
        String secondValue = "(Point(0 0)@2001-01-01 08:00:00+02, Point(1 2)@2001-01-03 08:00:00+02]";
        String thirdValue = "[Point(0 0)@2001-01-01 08:00:00+02, " +
                "Point(1 1)@2001-01-03 08:00:00+02, " +
                "Point(2 2)@2001-01-04 08:00:00+02)";

        TGeomPointSeq firstTemporal = new TGeomPointSeq(firstValue);
        TGeomPointSeq secondTemporal = new TGeomPointSeq(secondValue);
        TGeomPointSeq thirdTemporal = new TGeomPointSeq(thirdValue);

        assertNotEquals(firstTemporal, secondTemporal);
        assertNotEquals(firstTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, thirdTemporal);
        assertNotEquals(firstTemporal, new Object());
    }

    @Test
    void testSeqType() throws SQLException {
        String value = "[Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02)";
        TGeomPointSeq temporal = new TGeomPointSeq(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE, temporal.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        String value = String.format(
            "[SRID=4326;POINT(1 1)@2001-01-01 08:00:00%1$s, SRID=4326;POINT(2 2)@2001-01-03 08:00:00%1$s)",
            tz.toString().substring(0, 3)
        );
        TGeomPointSeq temporal = new TGeomPointSeq(value);
        String newValue = temporal.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        List<Point> list = tGeomPointSeq.getValues();
        Point firstPoint = new Point(0,0);
        Point secondPoint = new Point(1,1);
        Point thirdPoint = new Point(2,2);
        assertEquals(3 , list.size());
        assertEquals(firstPoint , list.get(0));
        assertEquals(secondPoint , list.get(1));
        assertEquals(thirdPoint , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        Point point = new Point(0,0);
        assertEquals(point, tGeomPointSeq.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        Point point = new Point(2,2);
        assertEquals(point, tGeomPointSeq.endValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertNull(tGeomPointSeq.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        Point point = new Point(0,0);
        assertEquals(point, tGeomPointSeq.valueAtTimestamp(timestamp));
    }
}
