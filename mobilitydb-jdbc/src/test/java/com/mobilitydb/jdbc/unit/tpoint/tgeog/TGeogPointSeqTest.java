package com.mobilitydb.jdbc.unit.tpoint.tgeog;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.tpoint.helpers.TPointConstants;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointInst;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointSeq;
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
    void testStepwiseConstructors() throws SQLException {
        String value = "Interp=Stepwise;[Point(1 1)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02)";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TGeogPointInst[] instants = new TGeogPointInst[]{
                new TGeogPointInst(new Point(1, 1), dateOne),
                new TGeogPointInst(new Point(1, 1), dateTwo)
        };
        String[] stringInstants = new String[]{
                "Point(1 1)@2001-01-01 08:00:00+02",
                "Point(1 1)@2001-01-03 08:00:00+02"
        };

        TGeogPointSeq firstTemporal = new TGeogPointSeq(value);
        TGeogPointSeq secondTemporal = new TGeogPointSeq(true, instants);
        TGeogPointSeq thirdTemporal = new TGeogPointSeq(true, stringInstants);
        TGeogPointSeq fourthTemporal = new TGeogPointSeq(true, instants, true, false);
        TGeogPointSeq fifthTemporal = new TGeogPointSeq(true, stringInstants, true, false);
        TGeogPointSeq sixthTemporal = new TGeogPointSeq(TPointConstants.DEFAULT_SRID, true, instants);
        TGeogPointSeq seventhTemporal = new TGeogPointSeq(TPointConstants.DEFAULT_SRID,true, stringInstants);
        TGeogPointSeq eighthTemporal = new TGeogPointSeq(TPointConstants.DEFAULT_SRID, true, instants, true, false);
        TGeogPointSeq ninthTemporal = new TGeogPointSeq(TPointConstants.DEFAULT_SRID, true, stringInstants, true, false);

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
        TGeogPointInst[] instants = new TGeogPointInst[]{
                new TGeogPointInst(new Point(1, 1), dateOne),
                new TGeogPointInst(new Point(2, 2), dateTwo)
        };
        String[] stringInstants = new String[]{
                "Point(1 1)@2001-01-01 08:00:00+02",
                "Point(2 2)@2001-01-03 08:00:00+02"
        };

        TGeogPointSeq firstTemporal = new TGeogPointSeq(value);
        TGeogPointSeq secondTemporal = new TGeogPointSeq(TPointConstants.DEFAULT_SRID, instants);
        TGeogPointSeq thirdTemporal = new TGeogPointSeq(TPointConstants.DEFAULT_SRID, stringInstants);
        TGeogPointSeq fourthTemporal = new TGeogPointSeq(TPointConstants.DEFAULT_SRID, instants, true, false);
        TGeogPointSeq fifthTemporal = new TGeogPointSeq(TPointConstants.DEFAULT_SRID, stringInstants, true, false);

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
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        String value = String.format(
            "[SRID=4326;POINT(1 1)@2001-01-01 08:00:00%1$s, SRID=4326;POINT(2 2)@2001-01-03 08:00:00%1$s)",
            format.format(tz)
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

    @Test
    void testNumTimestamps() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(3, tGeogPointSeq.numTimestamps());
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
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(3, tGeogPointSeq.timestamps().length);
        assertEquals(firstExpectedDate, tGeogPointSeq.timestamps()[0]);
        assertEquals(secondExpectedDate, tGeogPointSeq.timestamps()[1]);
        assertEquals(thirdExpectedDate, tGeogPointSeq.timestamps()[2]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(expectedDate, tGeogPointSeq.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeogPointSeq.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(expectedDate, tGeogPointSeq.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(expectedDate, tGeogPointSeq.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,true,false);
        assertEquals(period, tGeogPointSeq.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
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
        assertEquals(periodSet, tGeogPointSeq.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(3, tGeogPointSeq.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        TGeogPointInst tGeogPointInst = new TGeogPointInst("SRID=4326;Point(0 0)@2001-01-01 08:00:00+02");
        assertEquals(tGeogPointInst, tGeogPointSeq.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        TGeogPointInst tGeogPointInst = new TGeogPointInst("SRID=4326;Point(2 2)@2001-01-04 08:00:00+02");
        assertEquals(tGeogPointInst, tGeogPointSeq.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        TGeogPointInst tGeogPointInst = new TGeogPointInst("SRID=4326;Point(1 1)@2001-01-03 08:00:00+02");
        assertEquals(tGeogPointInst, tGeogPointSeq.instantN(1));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeogPointSeq.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        ArrayList<TGeogPointInst> list = new ArrayList<>();
        TGeogPointInst firstTGeogPointInst = new TGeogPointInst("Point(0 0)@2001-01-01 08:00:00+02");
        TGeogPointInst secondTGeogPointInst = new TGeogPointInst("Point(1 1)@2001-01-03 08:00:00+02");
        TGeogPointInst thirdTGeogPointInst = new TGeogPointInst("Point(2 2)@2001-01-04 08:00:00+02");
        list.add(firstTGeogPointInst);
        list.add(secondTGeogPointInst);
        list.add(thirdTGeogPointInst);
        assertEquals(3, list.size());
        assertEquals(list, tGeogPointSeq.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tGeogPointSeq.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tGeogPointSeq.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        TGeogPointSeq otherTGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-03 08:00:00+02, Point(1 1)@2001-01-05 08:00:00+02, Point(2 2)@2001-01-06 08:00:00+02)");
        tGeogPointSeq.shift(Duration.ofDays(2));
        assertEquals(otherTGeogPointSeq, tGeogPointSeq);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        assertTrue(tGeogPointSeq.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tGeogPointSeq.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tGeogPointSeq.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tGeogPointSeq.intersectsPeriod(period));
    }

    @Test
    void testNumSequences() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(1, tGeogPointSeq.numSequences());
    }

    @Test
    void testStartSequence() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(tGeogPointSeq, tGeogPointSeq.startSequence());
    }

    @Test
    void testEndSequence() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(tGeogPointSeq, tGeogPointSeq.endSequence());
    }

    @Test
    void testSequenceN() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        assertEquals(tGeogPointSeq, tGeogPointSeq.sequenceN(0));
    }

    @Test
    void testSequenceNNoValue() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeogPointSeq.sequenceN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no sequence at this index."));
    }

    @Test
    void testSequences() throws SQLException {
        TGeogPointSeq tGeogPointSeq = new TGeogPointSeq(
                "[Point(0 0)@2001-01-01 08:00:00+02, " +
                        "Point(1 1)@2001-01-03 08:00:00+02, " +
                        "Point(2 2)@2001-01-04 08:00:00+02)");
        ArrayList<TGeogPointSeq> list = new ArrayList<>();
        list.add(tGeogPointSeq);
        assertEquals(list, tGeogPointSeq.sequences());
    }
}
