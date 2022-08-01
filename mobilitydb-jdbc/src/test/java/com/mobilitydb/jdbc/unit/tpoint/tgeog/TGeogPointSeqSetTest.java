package com.mobilitydb.jdbc.unit.tpoint.tgeog;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.tpoint.helpers.TPointConstants;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointInst;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointSeq;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointSeqSet;
import org.junit.jupiter.api.Test;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
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
    void testStepwiseConstructors() throws SQLException {
        String value = "Interp=Stepwise;{[POINT(1 1)@2001-01-01 08:00:00+02, POINT(1 1)@2001-01-03 08:00:00+02), " +
                "[POINT(1 1)@2001-01-04 08:00:00+02, POINT(2 2)@2001-01-05 08:00:00+02, " +
                "POINT(3 3)@2001-01-06 08:00:00+02]}";

        TGeogPointSeq[] sequences = new TGeogPointSeq[]{
                new TGeogPointSeq("Interp=Stepwise;[POINT(1 1)@2001-01-01 08:00:00+02, POINT(1 1)@2001-01-03 08:00:00+02)"),
                new TGeogPointSeq("Interp=Stepwise;[" +
                        "POINT(1 1)@2001-01-04 08:00:00+02, " +
                        "POINT(2 2)@2001-01-05 08:00:00+02, " +
                        "POINT(3 3)@2001-01-06 08:00:00+02]")
        };
        String[] stringSequences = new String[]{
                "Interp=Stepwise;[POINT(1 1)@2001-01-01 08:00:00+02, POINT(1 1)@2001-01-03 08:00:00+02)",
                "Interp=Stepwise;[POINT(1 1)@2001-01-04 08:00:00+02, POINT(2 2)@2001-01-05 08:00:00+02, " +
                        "POINT(3 3)@2001-01-06 08:00:00+02]"
        };

        TGeogPointSeqSet firstTemporal = new TGeogPointSeqSet(value);
        TGeogPointSeqSet secondTemporal = new TGeogPointSeqSet(true, sequences);
        TGeogPointSeqSet thirdTemporal = new TGeogPointSeqSet(true, stringSequences);
        TGeogPointSeqSet fourthTemporal = new TGeogPointSeqSet(TPointConstants.DEFAULT_SRID,true, sequences);
        TGeogPointSeqSet fifthTemporal = new TGeogPointSeqSet(TPointConstants.DEFAULT_SRID,true, stringSequences);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
        assertEquals(firstTemporal, fourthTemporal);
        assertEquals(firstTemporal, fifthTemporal);
    }

    @Test
    void testSRIDConstructors() throws SQLException {
        String value = "SRID=4326;{[POINT(1 1)@2001-01-01 08:00:00+02, POINT(1 1)@2001-01-03 08:00:00+02), " +
                "[POINT(1 1)@2001-01-04 08:00:00+02, POINT(2 2)@2001-01-05 08:00:00+02, " +
                "POINT(3 3)@2001-01-06 08:00:00+02]}";

        TGeogPointSeq[] sequences = new TGeogPointSeq[]{
                new TGeogPointSeq("SRID=4326;[POINT(1 1)@2001-01-01 08:00:00+02, POINT(1 1)@2001-01-03 08:00:00+02)"),
                new TGeogPointSeq("SRID=4326;[" +
                        "POINT(1 1)@2001-01-04 08:00:00+02, " +
                        "POINT(2 2)@2001-01-05 08:00:00+02, " +
                        "POINT(3 3)@2001-01-06 08:00:00+02]")
        };
        String[] stringSequences = new String[]{
                "SRID=4326;[POINT(1 1)@2001-01-01 08:00:00+02, POINT(1 1)@2001-01-03 08:00:00+02)",
                "SRID=4326;[POINT(1 1)@2001-01-04 08:00:00+02, POINT(2 2)@2001-01-05 08:00:00+02, " +
                        "POINT(3 3)@2001-01-06 08:00:00+02]"
        };

        TGeogPointSeqSet firstTemporal = new TGeogPointSeqSet(value);
        TGeogPointSeqSet secondTemporal = new TGeogPointSeqSet(TPointConstants.DEFAULT_SRID, sequences);
        TGeogPointSeqSet thirdTemporal = new TGeogPointSeqSet(TPointConstants.DEFAULT_SRID, stringSequences);

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
        fourthPoint.setSrid(4326);
        Point fifthPoint = new Point(0,0);
        fifthPoint.setSrid(4326);
        assertEquals(5 , list.size());
        assertEquals(firstPoint , list.get(0));
        assertEquals(secondPoint , list.get(1));
        assertEquals(thirdPoint , list.get(2));
        assertEquals(fourthPoint , list.get(3));
        assertEquals(fifthPoint , list.get(4));
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

    @Test
    void testNumTimestamps() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        assertEquals(5, tGeogPointSeqSet.numTimestamps());
    }

    @Test
    void testTimestamps() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime firstExpectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime secondExpectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        OffsetDateTime thirdExpectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        OffsetDateTime fourthExpectedDate = OffsetDateTime.of(2001,1, 5,
                8, 0, 0, 0, tz);
        OffsetDateTime fifthExpectedDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        assertEquals(5, tGeogPointSeqSet.timestamps().length);
        assertEquals(firstExpectedDate, tGeogPointSeqSet.timestamps()[0]);
        assertEquals(secondExpectedDate, tGeogPointSeqSet.timestamps()[1]);
        assertEquals(thirdExpectedDate, tGeogPointSeqSet.timestamps()[2]);
        assertEquals(fourthExpectedDate, tGeogPointSeqSet.timestamps()[3]);
        assertEquals(fifthExpectedDate, tGeogPointSeqSet.timestamps()[4]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        assertEquals(expectedDate, tGeogPointSeqSet.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeogPointSeqSet.timestampN(6)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        assertEquals(expectedDate, tGeogPointSeqSet.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        assertEquals(expectedDate, tGeogPointSeqSet.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,true,true);
        assertEquals(period, tGeogPointSeqSet.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialExpectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endExpectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        Period firstPeriod = new Period(initialExpectedDate,endExpectedDate,true,false);
        initialExpectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        endExpectedDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        Period secondPeriod = new Period(initialExpectedDate,endExpectedDate,true,true);
        PeriodSet periodSet = new PeriodSet(firstPeriod, secondPeriod);
        assertEquals(periodSet, tGeogPointSeqSet.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        assertEquals(5, tGeogPointSeqSet.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        TGeogPointInst tGeogPointInst = new TGeogPointInst(
                "010100000000000000000000000000000000000000@2001-01-01 08:00:00+02");
        assertEquals(tGeogPointInst, tGeogPointSeqSet.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        TGeogPointInst tGeogPointInst = new TGeogPointInst(
                "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02");
        assertEquals(tGeogPointInst, tGeogPointSeqSet.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        TGeogPointInst tGeogPointInst = new TGeogPointInst(
                "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02");
        assertEquals(tGeogPointInst, tGeogPointSeqSet.instantN(3));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeogPointSeqSet.instantN(6)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        ArrayList<TGeogPointInst> list = new ArrayList<>();
        TGeogPointInst firstInst = new TGeogPointInst(
                "010100000000000000000000000000000000000000@2001-01-01 08:00:00+02");
        TGeogPointInst secondInst = new TGeogPointInst(
                "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02");
        TGeogPointInst thirdTInst = new TGeogPointInst(
                "010100000000000000000000000000000000000000@2001-01-04 08:00:00+02");
        TGeogPointInst fourthInst = new TGeogPointInst(
                "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02");
        TGeogPointInst fifthInst = new TGeogPointInst(
                "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02");
        list.add(firstInst);
        list.add(secondInst);
        list.add(thirdTInst);
        list.add(fourthInst);
        list.add(fifthInst);
        assertEquals(5, list.size());
        assertEquals(list, tGeogPointSeqSet.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        initialDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        endDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        expectedDuration = Duration.between(initialDate, endDate).plus(expectedDuration);
        assertEquals(expectedDuration, tGeogPointSeqSet.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tGeogPointSeqSet.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        TGeogPointSeqSet otherTGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-03 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-06 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-07 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-08 08:00:00+02]}");
        tGeogPointSeqSet.shift(Duration.ofDays(2));
        assertEquals(otherTGeogPointSeqSet, tGeogPointSeqSet);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        assertTrue(tGeogPointSeqSet.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tGeogPointSeqSet.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tGeogPointSeqSet.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tGeogPointSeqSet.intersectsPeriod(period));
    }

    @Test
    void testNumSequences() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        assertEquals(2, tGeogPointSeqSet.numSequences());
    }

    @Test
    void testStartSequence() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)");
        assertEquals(tGeogPointSeq, tGeogPointSeqSet.startSequence());
    }

    @Test
    void testEndSequence() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]");
        assertEquals(tGeogPointSeq, tGeogPointSeqSet.endSequence());
    }

    @Test
    void testSequenceN() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]");
        assertEquals(tGeogPointSeq, tGeogPointSeqSet.sequenceN(1));
    }

    @Test
    void testSequenceNNoValue() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeogPointSeqSet.sequenceN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no sequence at this index."));
    }

    @Test
    void testSequences() throws SQLException {
        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)," +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        ArrayList<TGeogPointSeq> list = new ArrayList<>();
        list.add(new TGeogPointSeq(
                "[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)"));
        list.add(new TGeogPointSeq(
                "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02," +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]"));
        assertEquals(list, tGeogPointSeqSet.sequences());
    }
}
