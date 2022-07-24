package com.mobilitydb.jdbc.unit.tpoint.tgeog;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointInst;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointSeq;
import org.junit.jupiter.api.Test;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TGeogPointSeqTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "[Point(1 1)@2001-01-01 08:00:00+02, Point(2 2)@2001-01-03 08:00:00+02)";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TGeogPointInst[] instants = new TGeogPointInst[]{
                new TGeogPointInst(new Point(1, 1), dateOne),
                new TGeogPointInst(new Point(2, 2), dateTwo)
        };
        String[] stringInstants = new String[]{
                "Point(1 1)@2001-01-01 08:00:00+02",
                "Point(2 2)@2001-01-03 08:00:00+02"
        };

        TGeogPointSeq firstTemporal = new TGeogPointSeq(value);
        TGeogPointSeq secondTemporal = new TGeogPointSeq(instants);
        TGeogPointSeq thirdTemporal = new TGeogPointSeq(stringInstants);
        TGeogPointSeq fourthTemporal = new TGeogPointSeq(instants, true, false);
        TGeogPointSeq fifthTemporal = new TGeogPointSeq(stringInstants, true, false);

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

        TGeogPointSeq firstTemporal = new TGeogPointSeq(firstValue);
        TGeogPointSeq secondTemporal = new TGeogPointSeq(secondValue);
        TGeogPointSeq thirdTemporal = new TGeogPointSeq(thirdValue);

        assertNotEquals(firstTemporal, secondTemporal);
        assertNotEquals(firstTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, thirdTemporal);
        assertNotEquals(firstTemporal, new Object());
    }

    @Test
    void testSeqType() throws SQLException {
        String value = "[Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02)";
        TGeogPointSeq temporal = new TGeogPointSeq(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE, temporal.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        String value = String.format(
            "[SRID=4326;POINT(1 1)@2001-01-01 08:00:00%1$s, SRID=4326;POINT(2 2)@2001-01-03 08:00:00%1$s)",
            tz.toString().substring(0, 3)
        );
        TGeogPointSeq temporal = new TGeogPointSeq(value);
        String newValue = temporal.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        List<Point> list = tGeogPointSeq.getValues();
        Point firstPoint = new Point(0,0);
        firstPoint.setSrid(4326);
        Point secondPoint = new Point(1,1);
        secondPoint.setSrid(4326);
        Point thirdPoint = new Point(2,2);
        thirdPoint.setSrid(4326);
        assertEquals(3 , list.size());
        assertEquals(firstPoint , list.get(0));
        assertEquals(secondPoint , list.get(1));
        assertEquals(thirdPoint , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        Point point = new Point(0,0);
        point.setSrid(4326);
        assertEquals(point, tGeogPointSeq.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        Point point = new Point(2,2);
        point.setSrid(4326);
        assertEquals(point, tGeogPointSeq.endValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertNull(tGeogPointSeq.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        Point point = new Point(0,0);
        point.setSrid(4326);
        assertEquals(point, tGeogPointSeq.valueAtTimestamp(timestamp));
    }
}
