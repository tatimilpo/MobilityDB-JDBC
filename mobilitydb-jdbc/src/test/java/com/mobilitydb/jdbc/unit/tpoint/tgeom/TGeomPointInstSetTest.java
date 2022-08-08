package com.mobilitydb.jdbc.unit.tpoint.tgeom;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.tpoint.helpers.TPointConstants;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointInst;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointInstSet;
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

class TGeomPointInstSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02}";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);

        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(value);
        TGeomPointInst[] instants = new TGeomPointInst[]{
            new TGeomPointInst(new Point(0, 0), dateOne),
            new TGeomPointInst(new Point (1, 1), dateTwo)
        };
        TGeomPointInstSet otherTGeomPointInstSet = new TGeomPointInstSet(instants);

        assertEquals(tGeomPointInstSet.getValues(), otherTGeomPointInstSet.getValues());
        assertEquals(tGeomPointInstSet, otherTGeomPointInstSet);
    }

    @Test
    void testSRIDConstructors() throws SQLException {
        String value = "SRID=4326;{Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02}";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);

        TGeomPointInstSet firstTemporal = new TGeomPointInstSet(value);
        TGeomPointInst[] instants = new TGeomPointInst[]{
                new TGeomPointInst(new Point(0, 0), dateOne),
                new TGeomPointInst(new Point (1, 1), dateTwo)
        };
        String[] values = new String[]{
                "SRID=4326;Point(0 0)@2001-01-01 08:00:00+02",
                "SRID=4326;Point(1 1)@2001-01-03 08:00:00+02"
        };
        TGeomPointInstSet secondTemporal = new TGeomPointInstSet(TPointConstants.DEFAULT_SRID, instants);
        TGeomPointInstSet thirdTemporal = new TGeomPointInstSet(TPointConstants.DEFAULT_SRID, values);

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

        TGeomPointInstSet firstTGeomPointInstSet = new TGeomPointInstSet(firstValue);
        TGeomPointInstSet secondTGeomPointInstSet = new TGeomPointInstSet(secondValue);
        TGeomPointInstSet thirdTGeomPointInstSet = new TGeomPointInstSet(thirdValue);

        assertNotEquals(firstTGeomPointInstSet, secondTGeomPointInstSet);
        assertNotEquals(firstTGeomPointInstSet, thirdTGeomPointInstSet);
        assertNotEquals(secondTGeomPointInstSet, thirdTGeomPointInstSet);
        assertNotEquals(firstTGeomPointInstSet, new Object());
    }

    @Test
    void testStringArrayStringConstructors() throws SQLException {
        String value = "{Point(0 0)@2001-01-01 08:00:00+02, Point(2 2)@2001-01-03 08:00:00+02}";

        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(value);
        String[] instants = new String[]{
            "Point(0 0)@2001-01-01 08:00:00+02",
            "Point(2 2)@2001-01-03 08:00:00+02"
        };
        TGeomPointInstSet otherTGeomPointInstSet = new TGeomPointInstSet(instants);

        assertEquals(tGeomPointInstSet.getValues(), otherTGeomPointInstSet.getValues());
    }

    @Test
    void testInstSetType() throws SQLException {
        String value = "{Point(0 0)@2001-01-01 08:00:00+02, Point(1 1)@2001-01-03 08:00:00+02}";
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tGeomPointInstSet.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        String value = String.format(
            "{SRID=4326;POINT(0 0)@2001-01-01 08:00:00%1$s, SRID=4326;POINT(1 1)@2001-01-03 08:00:00%1$s}",
            format.format(tz)
        );
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(value);
        String newValue = tGeomPointInstSet.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        List<Point> list = tGeomPointInstSet.getValues();
        Point firstPoint = new Point(1,0);
        Point secondPoint = new Point(0,2);
        Point thirdPoint = new Point(3,0);
        assertEquals(3 , list.size());
        assertEquals(firstPoint , list.get(0));
        assertEquals(secondPoint , list.get(1));
        assertEquals(thirdPoint , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        Point firstPoint = new Point(1,0);
        assertEquals(firstPoint, tGeomPointInstSet.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        Point thirdPoint = new Point(3,0);
        assertEquals(thirdPoint, tGeomPointInstSet.endValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        assertNull(tGeomPointInstSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        Point point = new Point(1,0);
        assertEquals(point, tGeomPointInstSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        assertEquals(3, tGeomPointInstSet.numTimestamps());
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
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        assertEquals(3, tGeomPointInstSet.timestamps().length);
        assertEquals(firstExpectedDate, tGeomPointInstSet.timestamps()[0]);
        assertEquals(secondExpectedDate, tGeomPointInstSet.timestamps()[1]);
        assertEquals(thirdExpectedDate, tGeomPointInstSet.timestamps()[2]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tGeomPointInstSet.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeomPointInstSet.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tGeomPointInstSet.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tGeomPointInstSet.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,true,true);
        assertEquals(period, tGeomPointInstSet.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
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
        assertEquals(periodSet, tGeomPointInstSet.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        assertEquals(3, tGeomPointInstSet.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(1 0)@2001-01-01 08:00:00+02");
        assertEquals(tGeomPointInst, tGeomPointInstSet.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(3 0)@2001-01-04 08:00:00+02");
        assertEquals(tGeomPointInst, tGeomPointInstSet.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        TGeomPointInst tGeomPointInst = new TGeomPointInst("Point(0 2)@2001-01-03 08:00:00+02");
        assertEquals(tGeomPointInst, tGeomPointInstSet.instantN(1));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tGeomPointInstSet.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        ArrayList<TGeomPointInst> list = new ArrayList<>();
        TGeomPointInst firstTGeomPointInst = new TGeomPointInst("Point(1 0)@2001-01-01 08:00:00+02");
        TGeomPointInst secondTGeomPointInst = new TGeomPointInst("Point(0 2)@2001-01-03 08:00:00+02");
        TGeomPointInst thirdTGeomPointInst = new TGeomPointInst("Point(3 0)@2001-01-04 08:00:00+02");
        list.add(firstTGeomPointInst);
        list.add(secondTGeomPointInst);
        list.add(thirdTGeomPointInst);
        assertEquals(3, list.size());
        assertEquals(list, tGeomPointInstSet.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        assertEquals(Duration.ZERO, tGeomPointInstSet.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tGeomPointInstSet.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        TGeomPointInstSet otherTGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-03 08:00:00+02, " +
                        "Point(0 2)@2001-01-05 08:00:00+02, " +
                        "Point(3 0)@2001-01-06 08:00:00+02}");
        tGeomPointInstSet.shift(Duration.ofDays(2));
        assertEquals(otherTGeomPointInstSet, tGeomPointInstSet);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        assertTrue(tGeomPointInstSet.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tGeomPointInstSet.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tGeomPointInstSet.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TGeomPointInstSet tGeomPointInstSet = new TGeomPointInstSet(
                "{Point(1 0)@2001-01-01 08:00:00+02, " +
                        "Point(0 2)@2001-01-03 08:00:00+02, " +
                        "Point(3 0)@2001-01-04 08:00:00+02}");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tGeomPointInstSet.intersectsPeriod(period));
    }
}
