package com.mobilitydb.jdbc.unit.tpoint.tgeom;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointInst;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointInstSet;
import org.junit.jupiter.api.Test;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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
        String value = String.format(
            "{SRID=4326;POINT(0 0)@2001-01-01 08:00:00%1$s, SRID=4326;POINT(1 1)@2001-01-03 08:00:00%1$s}",
            tz.toString().substring(0, 3)
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
}
