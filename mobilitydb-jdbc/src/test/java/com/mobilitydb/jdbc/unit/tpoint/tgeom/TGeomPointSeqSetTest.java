package com.mobilitydb.jdbc.unit.tpoint.tgeom;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.tpoint.helpers.TPointConstants;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointInst;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointSeq;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointSeqSet;
import org.junit.jupiter.api.Test;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
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
    void testStepwiseConstructors() throws SQLException {
        String value = "Interp=Stepwise;{[POINT(1 1)@2001-01-01 08:00:00+02, POINT(1 1)@2001-01-03 08:00:00+02), " +
                "[POINT(1 1)@2001-01-04 08:00:00+02, POINT(2 2)@2001-01-05 08:00:00+02, " +
                "POINT(3 3)@2001-01-06 08:00:00+02]}";

        TGeomPointSeq[] sequences = new TGeomPointSeq[]{
                new TGeomPointSeq("Interp=Stepwise;[POINT(1 1)@2001-01-01 08:00:00+02, POINT(1 1)@2001-01-03 08:00:00+02)"),
                new TGeomPointSeq("Interp=Stepwise;[" +
                        "POINT(1 1)@2001-01-04 08:00:00+02, " +
                        "POINT(2 2)@2001-01-05 08:00:00+02, " +
                        "POINT(3 3)@2001-01-06 08:00:00+02]")
        };
        String[] stringSequences = new String[]{
                "Interp=Stepwise;[POINT(1 1)@2001-01-01 08:00:00+02, POINT(1 1)@2001-01-03 08:00:00+02)",
                "Interp=Stepwise;[POINT(1 1)@2001-01-04 08:00:00+02, POINT(2 2)@2001-01-05 08:00:00+02, " +
                        "POINT(3 3)@2001-01-06 08:00:00+02]"
        };

        TGeomPointSeqSet firstTemporal = new TGeomPointSeqSet(value);
        TGeomPointSeqSet secondTemporal = new TGeomPointSeqSet(true, sequences);
        TGeomPointSeqSet thirdTemporal = new TGeomPointSeqSet(true, stringSequences);
        TGeomPointSeqSet fourthTemporal = new TGeomPointSeqSet(TPointConstants.EMPTY_SRID,true, sequences);
        TGeomPointSeqSet fifthTemporal = new TGeomPointSeqSet(TPointConstants.EMPTY_SRID,true, stringSequences);

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

        TGeomPointSeq[] sequences = new TGeomPointSeq[]{
                new TGeomPointSeq("SRID=4326;[POINT(1 1)@2001-01-01 08:00:00+02, POINT(1 1)@2001-01-03 08:00:00+02)"),
                new TGeomPointSeq("SRID=4326;[" +
                        "POINT(1 1)@2001-01-04 08:00:00+02, " +
                        "POINT(2 2)@2001-01-05 08:00:00+02, " +
                        "POINT(3 3)@2001-01-06 08:00:00+02]")
        };
        String[] stringSequences = new String[]{
                "SRID=4326;[POINT(1 1)@2001-01-01 08:00:00+02, POINT(1 1)@2001-01-03 08:00:00+02)",
                "SRID=4326;[POINT(1 1)@2001-01-04 08:00:00+02, POINT(2 2)@2001-01-05 08:00:00+02, " +
                        "POINT(3 3)@2001-01-06 08:00:00+02]"
        };

        TGeomPointSeqSet firstTemporal = new TGeomPointSeqSet(value);
        TGeomPointSeqSet secondTemporal = new TGeomPointSeqSet(TPointConstants.DEFAULT_SRID, sequences);
        TGeomPointSeqSet thirdTemporal = new TGeomPointSeqSet(TPointConstants.DEFAULT_SRID, stringSequences);

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

    @Test
    void testNumTimestamps() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        assertEquals(5, tGeomPointSeqSet.numTimestamps());
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
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        assertEquals(5, tGeomPointSeqSet.timestamps().length);
        assertEquals(firstExpectedDate, tGeomPointSeqSet.timestamps()[0]);
        assertEquals(secondExpectedDate, tGeomPointSeqSet.timestamps()[1]);
        assertEquals(thirdExpectedDate, tGeomPointSeqSet.timestamps()[2]);
        assertEquals(fourthExpectedDate, tGeomPointSeqSet.timestamps()[3]);
        assertEquals(fifthExpectedDate, tGeomPointSeqSet.timestamps()[4]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        assertEquals(expectedDate, tGeomPointSeqSet.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeomPointSeqSet.timestampN(6)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        assertEquals(expectedDate, tGeomPointSeqSet.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        assertEquals(expectedDate, tGeomPointSeqSet.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,true,true);
        assertEquals(period, tGeomPointSeqSet.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
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
        assertEquals(periodSet, tGeomPointSeqSet.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        assertEquals(5, tGeomPointSeqSet.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        TGeomPointInst tGeomPointInst = new TGeomPointInst(
                "010100000000000000000000000000000000000000@2001-01-01 08:00:00+02");
        assertEquals(tGeomPointInst, tGeomPointSeqSet.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        TGeomPointInst tGeomPointInst = new TGeomPointInst(
                "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02");
        assertEquals(tGeomPointInst, tGeomPointSeqSet.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        TGeomPointInst tGeomPointInst = new TGeomPointInst(
                "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02");
        assertEquals(tGeomPointInst, tGeomPointSeqSet.instantN(3));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeomPointSeqSet.instantN(6)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        ArrayList<TGeomPointInst> list = new ArrayList<>();
        TGeomPointInst firstInst = new TGeomPointInst(
                "010100000000000000000000000000000000000000@2001-01-01 08:00:00+02");
        TGeomPointInst secondInst = new TGeomPointInst(
                "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02");
        TGeomPointInst thirdTInst = new TGeomPointInst(
                "010100000000000000000000000000000000000000@2001-01-04 08:00:00+02");
        TGeomPointInst fourthInst = new TGeomPointInst(
                "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02");
        TGeomPointInst fifthInst = new TGeomPointInst(
                "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02");
        list.add(firstInst);
        list.add(secondInst);
        list.add(thirdTInst);
        list.add(fourthInst);
        list.add(fifthInst);
        assertEquals(5, list.size());
        assertEquals(list, tGeomPointSeqSet.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
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
        assertEquals(expectedDuration, tGeomPointSeqSet.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tGeomPointSeqSet.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        TGeomPointSeqSet otherTGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-03 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-06 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-07 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-08 08:00:00+02]}");
        tGeomPointSeqSet.shift(Duration.ofDays(2));
        assertEquals(otherTGeomPointSeqSet, tGeomPointSeqSet);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        assertTrue(tGeomPointSeqSet.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tGeomPointSeqSet.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tGeomPointSeqSet.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tGeomPointSeqSet.intersectsPeriod(period));
    }

    @Test
    void testNumSequences() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        assertEquals(2, tGeomPointSeqSet.numSequences());
    }

    @Test
    void testStartSequence() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)");
        assertEquals(tGeomPointSeq, tGeomPointSeqSet.startSequence());
    }

    @Test
    void testEndSequence() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]");
        assertEquals(tGeomPointSeq, tGeomPointSeqSet.endSequence());
    }

    @Test
    void testSequenceN() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]");
        assertEquals(tGeomPointSeq, tGeomPointSeqSet.sequenceN(1));
    }

    @Test
    void testSequenceNNoValue() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeomPointSeqSet.sequenceN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no sequence at this index."));
    }

    @Test
    void testSequences() throws SQLException {
        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(
                "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                        "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}");
        ArrayList<TGeomPointSeq> list = new ArrayList<>();
        list.add(new TGeomPointSeq(
                "[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02," +
                        "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02)"));
        list.add(new TGeomPointSeq(
                "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02," +
                        "010100000000000000000000000000000000000000@2001-01-05 08:00:00+02, " +
                        "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]"));
        assertEquals(list, tGeomPointSeqSet.sequences());
    }
}
