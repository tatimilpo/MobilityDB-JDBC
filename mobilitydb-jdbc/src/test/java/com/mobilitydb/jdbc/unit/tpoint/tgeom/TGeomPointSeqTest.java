package com.mobilitydb.jdbc.unit.tpoint.tgeom;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.tpoint.helpers.TPointConstants;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointInst;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointSeq;
import org.junit.jupiter.api.Test;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
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
    void testStepwiseConstructors() throws SQLException {
        String value = "Interp=Stepwise;[Point(1 1)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02)";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TGeomPointInst[] instants = new TGeomPointInst[]{
                new TGeomPointInst(new Point(1, 1), dateOne),
                new TGeomPointInst(new Point(1, 1), dateTwo)
        };
        String[] stringInstants = new String[]{
                "Point(1 1)@2001-01-01 08:00:00+02",
                "Point(1 1)@2001-01-03 08:00:00+02"
        };

        TGeomPointSeq firstTemporal = new TGeomPointSeq(value);
        TGeomPointSeq secondTemporal = new TGeomPointSeq(true, instants);
        TGeomPointSeq thirdTemporal = new TGeomPointSeq(true, stringInstants);
        TGeomPointSeq fourthTemporal = new TGeomPointSeq(true, instants, true, false);
        TGeomPointSeq fifthTemporal = new TGeomPointSeq(true, stringInstants, true, false);
        TGeomPointSeq sixthTemporal = new TGeomPointSeq(TPointConstants.EMPTY_SRID, true, instants);
        TGeomPointSeq seventhTemporal = new TGeomPointSeq(TPointConstants.EMPTY_SRID,true, stringInstants);
        TGeomPointSeq eighthTemporal = new TGeomPointSeq(TPointConstants.EMPTY_SRID, true, instants, true, false);
        TGeomPointSeq ninthTemporal = new TGeomPointSeq(TPointConstants.EMPTY_SRID, true, stringInstants, true, false);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
        assertEquals(firstTemporal, fourthTemporal);
        assertEquals(firstTemporal, fifthTemporal);
        assertEquals(firstTemporal, sixthTemporal);
        assertEquals(firstTemporal, seventhTemporal);
        assertEquals(firstTemporal, eighthTemporal);
        assertEquals(firstTemporal, ninthTemporal);
    }

    @Test
    void testSRIDConstructors() throws SQLException {
        String value = "SRID=4326;[Point(1 1)@2001-01-01 08:00:00+02, Point(2 2)@2001-01-03 08:00:00+02)";
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
        TGeomPointSeq secondTemporal = new TGeomPointSeq(TPointConstants.DEFAULT_SRID, instants);
        TGeomPointSeq thirdTemporal = new TGeomPointSeq(TPointConstants.DEFAULT_SRID, stringInstants);
        TGeomPointSeq fourthTemporal = new TGeomPointSeq(TPointConstants.DEFAULT_SRID, instants, true, false);
        TGeomPointSeq fifthTemporal = new TGeomPointSeq(TPointConstants.DEFAULT_SRID, stringInstants, true, false);

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
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        String value = String.format(
            "[SRID=4326;POINT(1 1)@2001-01-01 08:00:00%1$s, SRID=4326;POINT(2 2)@2001-01-03 08:00:00%1$s)",
            format.format(tz)
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

    @Test
    void testNumTimestamps() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(3, tGeomPointSeq.numTimestamps());
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
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(3, tGeomPointSeq.timestamps().length);
        assertEquals(firstExpectedDate, tGeomPointSeq.timestamps()[0]);
        assertEquals(secondExpectedDate, tGeomPointSeq.timestamps()[1]);
        assertEquals(thirdExpectedDate, tGeomPointSeq.timestamps()[2]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(expectedDate, tGeomPointSeq.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeomPointSeq.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(expectedDate, tGeomPointSeq.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(expectedDate, tGeomPointSeq.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,true,false);
        assertEquals(period, tGeomPointSeq.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialExpectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endExpectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);

        Period period = new Period(initialExpectedDate,endExpectedDate,true,false);
        PeriodSet periodSet = new PeriodSet(period);
        assertEquals(periodSet, tGeomPointSeq.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(3, tGeomPointSeq.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 0)@2001-01-01 08:00:00+02");
        assertEquals(tGeomPointInst, tGeomPointSeq.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(2 2)@2001-01-04 08:00:00+02");
        assertEquals(tGeomPointInst, tGeomPointSeq.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(1 1)@2001-01-03 08:00:00+02");
        assertEquals(tGeomPointInst, tGeomPointSeq.instantN(1));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeomPointSeq.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        ArrayList<TGeomPointInst> list = new ArrayList<>();
        TGeomPointInst firstTGeomPointInst = new TGeomPointInst("Point(0 0)@2001-01-01 08:00:00+02");
        TGeomPointInst secondTGeomPointInst = new TGeomPointInst("Point(1 1)@2001-01-03 08:00:00+02");
        TGeomPointInst thirdTGeomPointInst = new TGeomPointInst("Point(2 2)@2001-01-04 08:00:00+02");
        list.add(firstTGeomPointInst);
        list.add(secondTGeomPointInst);
        list.add(thirdTGeomPointInst);
        assertEquals(3, list.size());
        assertEquals(list, tGeomPointSeq.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tGeomPointSeq.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tGeomPointSeq.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        TGeomPointSeq otherTGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-03 08:00:00+02, " +
                        "Point(1 1)@2001-01-05 08:00:00+02, " +
                        "Point(2 2)@2001-01-06 08:00:00+02)");
        tGeomPointSeq.shift(Duration.ofDays(2));
        assertEquals(otherTGeomPointSeq, tGeomPointSeq);
    }
    @Test
    void testIntersectsTimestamp() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        assertTrue(tGeomPointSeq.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tGeomPointSeq.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tGeomPointSeq.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tGeomPointSeq.intersectsPeriod(period));
    }


    @Test
    void testNumSequences() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(1, tGeomPointSeq.numSequences());
    }

    @Test
    void testStartSequence() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(tGeomPointSeq, tGeomPointSeq.startSequence());
    }

    @Test
    void testEndSequence() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(tGeomPointSeq, tGeomPointSeq.endSequence());
    }

    @Test
    void testSequenceN() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(tGeomPointSeq, tGeomPointSeq.sequenceN(0));
    }

    @Test
    void testSequenceNNoValue() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeomPointSeq.sequenceN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no sequence at this index."));
    }

    @Test
    void testSequences() throws SQLException {
        TGeomPointSeq tGeomPointSeq = new TGeomPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        ArrayList<TGeomPointSeq> list = new ArrayList<>();
        list.add(tGeomPointSeq);
        assertEquals(list, tGeomPointSeq.sequences());
    }
}
