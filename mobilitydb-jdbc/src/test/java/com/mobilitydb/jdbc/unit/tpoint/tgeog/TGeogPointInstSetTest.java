package com.mobilitydb.jdbc.unit.tpoint.tgeog;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.tpoint.helpers.TPointConstants;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointInst;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointInstSet;
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

class TGeogPointInstSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02}";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);

        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(value);
        TGeogPointInst[] instants = new TGeogPointInst[]{
            new TGeogPointInst(new Point(0, 0), dateOne),
            new TGeogPointInst(new Point (1, 1), dateTwo)
        };
        TGeogPointInstSet otherTGeogPointInstSet = new TGeogPointInstSet(instants);

        assertEquals(tGeogPointInstSet.getValues(), otherTGeogPointInstSet.getValues());
        assertEquals(tGeogPointInstSet, otherTGeogPointInstSet);
    }

    @Test
    void testSRIDConstructors() throws SQLException {
        String value = "SRID=4326;{Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02}";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);

        TGeogPointInstSet firstTemporal = new TGeogPointInstSet(value);
        TGeogPointInst[] instants = new TGeogPointInst[]{
                new TGeogPointInst(new Point(0, 0), dateOne),
                new TGeogPointInst(new Point (1, 1), dateTwo)
        };
        String[] values = new String[]{
                "SRID=4326;Point(0 0)@2001-01-01 08:00:00+02",
                "SRID=4326;Point(1 1)@2001-01-03 08:00:00+02"
        };
        TGeogPointInstSet secondTemporal = new TGeogPointInstSet(TPointConstants.DEFAULT_SRID, instants);
        TGeogPointInstSet thirdTemporal = new TGeogPointInstSet(TPointConstants.DEFAULT_SRID, values);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02}";
        String secondValue = "{Point(0 1)@2001-01-01 08:00:00+02, Point(1 0)@2001-01-03 08:00:00+02}";
        String thirdValue = "{Point(1 0)@2001-01-01 08:00:00+02, " +
                "Point(0 2)@2001-01-03 08:00:00+02, " +
                "Point(3 0)@2001-01-04 08:00:00+02}";

        TGeogPointInstSet firstTGeogPointInstSet = new TGeogPointInstSet(firstValue);
        TGeogPointInstSet secondTGeogPointInstSet = new TGeogPointInstSet(secondValue);
        TGeogPointInstSet thirdTGeogPointInstSet = new TGeogPointInstSet(thirdValue);

        assertNotEquals(firstTGeogPointInstSet, secondTGeogPointInstSet);
        assertNotEquals(firstTGeogPointInstSet, thirdTGeogPointInstSet);
        assertNotEquals(secondTGeogPointInstSet, thirdTGeogPointInstSet);
        assertNotEquals(firstTGeogPointInstSet, new Object());
    }

    @Test
    void testStringArrayStringConstructors() throws SQLException {
        String value = "{Point(0 0)@2001-01-01 08:00:00+02, Point(2 2)@2001-01-03 08:00:00+02}";

        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(value);
        String[] instants = new String[]{
            "Point(0 0)@2001-01-01 08:00:00+02",
            "Point(2 2)@2001-01-03 08:00:00+02"
        };
        TGeogPointInstSet otherTGeogPointInstSet = new TGeogPointInstSet(instants);

        assertEquals(tGeogPointInstSet.getValues(), otherTGeogPointInstSet.getValues());
    }

    @Test
    void testInstSetType() throws SQLException {
        String value = "{Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02}";
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tGeogPointInstSet.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        String value = String.format(
            "{SRID=4326;POINT(0 0)@2001-01-01 08:00:00%1$s, SRID=4326;POINT(1 1)@2001-01-03 08:00:00%1$s}",
            format.format(tz)
        );
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(value);
        String newValue = tGeogPointInstSet.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        List<Point> list = tGeogPointInstSet.getValues();
        Point firstPoint = new Point(1,0);
        firstPoint.setSrid(4326);
        Point secondPoint = new Point(0,2);
        secondPoint.setSrid(4326);
        Point thirdPoint = new Point(3,0);
        thirdPoint.setSrid(4326);
        assertEquals(3 , list.size());
        assertEquals(firstPoint , list.get(0));
        assertEquals(secondPoint , list.get(1));
        assertEquals(thirdPoint , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        Point firstPoint = new Point(1,0);
        firstPoint.setSrid(4326);
        assertEquals(firstPoint, tGeogPointInstSet.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        Point thirdPoint = new Point(3,0);
        thirdPoint.setSrid(4326);
        assertEquals(thirdPoint, tGeogPointInstSet.endValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        assertNull(tGeogPointInstSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        Point point = new Point(1,0);
        point.setSrid(4326);
        assertEquals(point, tGeogPointInstSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        assertEquals(3, tGeogPointInstSet.numTimestamps());
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
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        assertEquals(3, tGeogPointInstSet.timestamps().length);
        assertEquals(firstExpectedDate, tGeogPointInstSet.timestamps()[0]);
        assertEquals(secondExpectedDate, tGeogPointInstSet.timestamps()[1]);
        assertEquals(thirdExpectedDate, tGeogPointInstSet.timestamps()[2]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tGeogPointInstSet.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeogPointInstSet.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tGeogPointInstSet.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tGeogPointInstSet.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,true,true);
        assertEquals(period, tGeogPointInstSet.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime firstExpectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime secondExpectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        OffsetDateTime thirdExpectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);

        Period firstPeriod = new Period(firstExpectedDate, firstExpectedDate, true, true);
        Period secondPeriod = new Period(secondExpectedDate, secondExpectedDate, true, true);
        Period thirdPeriod = new Period(thirdExpectedDate, thirdExpectedDate, true, true);

        PeriodSet periodSet = new PeriodSet(firstPeriod,secondPeriod,thirdPeriod);
        assertEquals(periodSet, tGeogPointInstSet.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        assertEquals(3, tGeogPointInstSet.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(1 0)@2001-01-01 08:00:00+02");
        assertEquals(tGeogPointInst, tGeogPointInstSet.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(3 0)@2001-01-04 08:00:00+02");
        assertEquals(tGeogPointInst, tGeogPointInstSet.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        TGeogPointInst tGeogPointInst = new TGeogPointInst("Point(0 2)@2001-01-03 08:00:00+02");
        assertEquals(tGeogPointInst, tGeogPointInstSet.instantN(1));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeogPointInstSet.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        ArrayList<TGeogPointInst> list = new ArrayList<>();
        TGeogPointInst firstTGeogPointInst = new TGeogPointInst("Point(1 0)@2001-01-01 08:00:00+02");
        TGeogPointInst secondTGeogPointInst = new TGeogPointInst("Point(0 2)@2001-01-03 08:00:00+02");
        TGeogPointInst thirdTGeogPointInst = new TGeogPointInst("Point(3 0)@2001-01-04 08:00:00+02");
        list.add(firstTGeogPointInst);
        list.add(secondTGeogPointInst);
        list.add(thirdTGeogPointInst);
        assertEquals(3, list.size());
        assertEquals(list, tGeogPointInstSet.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        assertEquals(Duration.ZERO, tGeogPointInstSet.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tGeogPointInstSet.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        TGeogPointInstSet otherTGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-03 08:00:00+02, " +
                        "Point(0 2)@2001-01-05 08:00:00+02, " +
                        "Point(3 0)@2001-01-06 08:00:00+02}");
        tGeogPointInstSet.shift(Duration.ofDays(2));
        assertEquals(otherTGeogPointInstSet, tGeogPointInstSet);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        assertTrue(tGeogPointInstSet.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tGeogPointInstSet.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tGeogPointInstSet.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TGeogPointInstSet tGeogPointInstSet = new TGeogPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tGeogPointInstSet.intersectsPeriod(period));
    }
}
