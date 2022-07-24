package com.mobilitydb.jdbc.unit.tpoint.tgeom;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointSeq;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointSeqSet;
import org.junit.jupiter.api.Test;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TGeomPointSeqSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{" +
            "[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
            "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}";

        TGeomPointSeq[] sequences = new TGeomPointSeq[]{
            new TGeomPointSeq("[" +
                    "010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)"),
            new TGeomPointSeq("[" +
                    "010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]")
        };
        String[] stringSequences = new String[]{
            "[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)",
            "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]"
        };

        TGeomPointSeqSet firstTemporal = new TGeomPointSeqSet(value);
        TGeomPointSeqSet secondTemporal = new TGeomPointSeqSet(sequences);
        TGeomPointSeqSet thirdTemporal = new TGeomPointSeqSet(stringSequences);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}";
        String secondValue = "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)}";
        String thirdValue = "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02]}";

        TGeomPointSeqSet firstTemporal = new TGeomPointSeqSet(firstValue);
        TGeomPointSeqSet secondTemporal = new TGeomPointSeqSet(secondValue);
        TGeomPointSeqSet thirdTemporal = new TGeomPointSeqSet(thirdValue);

        assertNotEquals(firstTemporal, secondTemporal);
        assertNotEquals(firstTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, thirdTemporal);
        assertNotEquals(firstTemporal, new Object());
    }

    @Test
    void testSeqSetType() throws SQLException {
        String value = "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}";
        TGeomPointSeqSet temporal = new TGeomPointSeqSet(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE_SET, temporal.getTemporalType());
    }

    @Test
    void testGetValues() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        List<Point> list = tGeomPointSeqSet.getValues();
        Point firstPoint = new Point(0,0);
        Point secondPoint = new Point(0,0);
        Point thirdPoint = new Point(0,0);
        Point fourthPoint = new Point(0,0);
        Point fifthPoint = new Point(0,0);
        assertEquals(5 , list.size());
        assertEquals(firstPoint , list.get(0));
        assertEquals(secondPoint , list.get(1));
        assertEquals(thirdPoint , list.get(2));
        assertEquals(fourthPoint , list.get(3));
        assertEquals(fifthPoint , list.get(4));
    }

    @Test
    void testStartValue() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        Point point = new Point(0,0);
        assertEquals(point, tGeomPointSeqSet.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        Point point = new Point(0,0);
        assertEquals(point, tGeomPointSeqSet.endValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        assertNull(tGeomPointSeqSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        Point point = new Point(0,0);
        assertEquals(point, tGeomPointSeqSet.valueAtTimestamp(timestamp));
    }
}
