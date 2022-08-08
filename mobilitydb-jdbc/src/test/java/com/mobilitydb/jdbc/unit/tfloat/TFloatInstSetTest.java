package com.mobilitydb.jdbc.unit.tfloat;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tfloat.TFloatInst;
import com.mobilitydb.jdbc.tfloat.TFloatInstSet;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TFloatInstSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{1.8@2001-01-01 08:00:00+02, 2.8@2001-01-03 08:00:00+02}";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);

        TFloatInstSet tFloatInstSet = new TFloatInstSet(value);
        TFloatInst[] instants = new TFloatInst[]{
                new TFloatInst(1.8f, dateOne),
                new TFloatInst(2.8f, dateTwo)
        };
        TFloatInstSet otherTIntInstSet = new TFloatInstSet(instants);

        assertEquals(tFloatInstSet.getValues(), otherTIntInstSet.getValues());
        assertEquals(tFloatInstSet, otherTIntInstSet);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{1.45@2001-01-01 08:00:00+02, 2.2@2001-01-03 08:00:00+02}";
        String secondValue = "{2.69@2001-01-01 08:00:00+02, 3.85@2001-01-03 08:00:00+02}";
        String thirdValue = "{1.77@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}";

        TFloatInstSet firstTFloatInstSet = new TFloatInstSet(firstValue);
        TFloatInstSet secondTFloatInstSet = new TFloatInstSet(secondValue);
        TFloatInstSet thirdTFloatInstSet = new TFloatInstSet(thirdValue);

        assertNotEquals(firstTFloatInstSet, secondTFloatInstSet);
        assertNotEquals(firstTFloatInstSet, thirdTFloatInstSet);
        assertNotEquals(secondTFloatInstSet, thirdTFloatInstSet);
        assertNotEquals(firstTFloatInstSet, new Object());
    }

    @Test
    void testStringArrayStringConstructors() throws SQLException {
        String value = "{19.8@2001-01-01 08:00:00+02, 2.52@2001-01-03 08:00:00+02}";

        TFloatInstSet tFloatInstSet = new TFloatInstSet(value);
        String[] instants = new String[]{
                "19.8@2001-01-01 08:00:00+02",
                "2.52@2001-01-03 08:00:00+02"
        };
        TFloatInstSet otherTFloatInstSet = new TFloatInstSet(instants);

        assertEquals(tFloatInstSet.getValues(), otherTFloatInstSet.getValues());
    }

    @Test
    void testInstSetType() throws SQLException {
        String value = "{1.11@2001-01-01 08:00:00+02, 2.22@2001-01-03 08:00:00+02}";
        TFloatInstSet tFloatInstSet = new TFloatInstSet(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tFloatInstSet.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        String value = String.format(
                "{47.5@2001-01-01 08:00:00%1$s, 96.5@2001-01-03 08:00:00%1$s}",
                format.format(tz)
        );
        TFloatInstSet tFloatInstSet = new TFloatInstSet(value);
        String newValue = tFloatInstSet.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.77@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        List<Float> list = tFloatInstSet.getValues();
        assertEquals(3 , list.size());
        assertEquals(1.77f , list.get(0));
        assertEquals(2.1f , list.get(1));
        assertEquals(3.78f , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.77@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertEquals(1.77f, tFloatInstSet.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.77@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertEquals(3.78f, tFloatInstSet.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.77@2001-01-01 08:00:00+02, 0.5@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertEquals(0.5f, tFloatInstSet.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{8.9@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertEquals(8.9f, tFloatInstSet.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.77@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertNull(tFloatInstSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertEquals(1.82f, tFloatInstSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertEquals(3, tFloatInstSet.numTimestamps());
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
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertEquals(3, tFloatInstSet.timestamps().length);
        assertEquals(firstExpectedDate, tFloatInstSet.timestamps()[0]);
        assertEquals(secondExpectedDate, tFloatInstSet.timestamps()[1]);
        assertEquals(thirdExpectedDate, tFloatInstSet.timestamps()[2]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tFloatInstSet.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tFloatInstSet.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tFloatInstSet.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tFloatInstSet.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,true,true);
        assertEquals(period, tFloatInstSet.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
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
        assertEquals(periodSet, tFloatInstSet.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertEquals(3, tFloatInstSet.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        TFloatInst tFloatInst = new TFloatInst("1.82@2001-01-01 08:00:00+02");
        assertEquals(tFloatInst, tFloatInstSet.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        TFloatInst tFloatInst = new TFloatInst("3.78@2001-01-04 08:00:00+02");
        assertEquals(tFloatInst, tFloatInstSet.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        TFloatInst tFloatInst = new TFloatInst("2.1@2001-01-03 08:00:00+02");
        assertEquals(tFloatInst, tFloatInstSet.instantN(1));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tFloatInstSet.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        ArrayList<TFloatInst> list = new ArrayList<>();
        TFloatInst firstTFloatInst = new TFloatInst("1.82@2001-01-01 08:00:00+02");
        TFloatInst secondTFloatInst = new TFloatInst("2.1@2001-01-03 08:00:00+02");
        TFloatInst thirdTFloatInst = new TFloatInst("3.78@2001-01-04 08:00:00+02");
        list.add(firstTFloatInst);
        list.add(secondTFloatInst);
        list.add(thirdTFloatInst);
        assertEquals(3, list.size());
        assertEquals(list, tFloatInstSet.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertEquals(Duration.ZERO, tFloatInstSet.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tFloatInstSet.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        TFloatInstSet otherTFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-03 08:00:00+02, 2.1@2001-01-05 08:00:00+02, 3.78@2001-01-06 08:00:00+02}");
        tFloatInstSet.shift(Duration.ofDays(2));
        assertEquals(otherTFloatInstSet, tFloatInstSet);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        assertTrue(tFloatInstSet.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tFloatInstSet.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tFloatInstSet.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tFloatInstSet.intersectsPeriod(period));
    }
}
