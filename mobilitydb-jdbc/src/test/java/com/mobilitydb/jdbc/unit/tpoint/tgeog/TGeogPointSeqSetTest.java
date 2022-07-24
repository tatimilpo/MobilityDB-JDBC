package com.mobilitydb.jdbc.unit.tpoint.tgeog;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointSeq;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointSeqSet;
import org.junit.jupiter.api.Test;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TGeogPointSeqSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{" +
            "[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
            "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}";

        TGeogPointSeq[] sequences = new TGeogPointSeq[]{
            new TGeogPointSeq("[" +
                    "010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)"),
            new TGeogPointSeq("[" +
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

        TGeogPointSeqSet firstTemporal = new TGeogPointSeqSet(value);
        TGeogPointSeqSet secondTemporal = new TGeogPointSeqSet(sequences);
        TGeogPointSeqSet thirdTemporal = new TGeogPointSeqSet(stringSequences);

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

        TGeogPointSeqSet firstTemporal = new TGeogPointSeqSet(firstValue);
        TGeogPointSeqSet secondTemporal = new TGeogPointSeqSet(secondValue);
        TGeogPointSeqSet thirdTemporal = new TGeogPointSeqSet(thirdValue);

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
        TGeogPointSeqSet temporal = new TGeogPointSeqSet(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE_SET, temporal.getTemporalType());
    }

    @Test
    void testGetValues() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        List<Point> list = tGeogPointSeqSet.getValues();
        Point firstPoint = new Point(0,0);
        firstPoint.setSrid(4326);
        Point secondPoint = new Point(0,0);
        secondPoint.setSrid(4326);
        Point thirdPoint = new Point(0,0);
        thirdPoint.setSrid(4326);
        Point fourthPoint = new Point(0,0);
        secondPoint.setSrid(4326);
        Point fifthPoint = new Point(0,0);
        thirdPoint.setSrid(4326);
        assertEquals(5 , list.size());
        assertEquals(firstPoint , list.get(0));
        assertEquals(secondPoint , list.get(1));
        assertEquals(thirdPoint , list.get(2));
        assertEquals(secondPoint , list.get(3));
        assertEquals(thirdPoint , list.get(4));
    }

    @Test
    void testStartValue() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        Point point = new Point(0,0);
        point.setSrid(4326);
        assertEquals(point, tGeogPointSeqSet.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        Point point = new Point(0,0);
        point.setSrid(4326);
        assertEquals(point, tGeogPointSeqSet.endValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        assertNull(tGeogPointSeqSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        Point point = new Point(0,0);
        point.setSrid(4326);
        assertEquals(point, tGeogPointSeqSet.valueAtTimestamp(timestamp));
    }
}
