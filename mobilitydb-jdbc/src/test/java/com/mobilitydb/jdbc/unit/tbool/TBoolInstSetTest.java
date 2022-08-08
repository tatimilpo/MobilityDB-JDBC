package com.mobilitydb.jdbc.unit.tbool;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tbool.TBoolInst;
import com.mobilitydb.jdbc.tbool.TBoolInstSet;
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

class TBoolInstSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02}";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);

        TBoolInstSet tBoolInstSet = new TBoolInstSet(value);
        TBoolInst[] instants = new TBoolInst[]{
            new TBoolInst(false, dateOne),
            new TBoolInst(true, dateTwo)
        };
        TBoolInstSet otherTBoolInstSet = new TBoolInstSet(instants);

        assertEquals(tBoolInstSet.getValues(), otherTBoolInstSet.getValues());
        assertEquals(tBoolInstSet, otherTBoolInstSet);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02}";
        String secondValue = "{true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02}";
        String thirdValue = "{false@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}";

        TBoolInstSet firstTBoolInstSet = new TBoolInstSet(firstValue);
        TBoolInstSet secondTBoolInstSet = new TBoolInstSet(secondValue);
        TBoolInstSet thirdTBoolInstSet = new TBoolInstSet(thirdValue);

        assertNotEquals(firstTBoolInstSet, secondTBoolInstSet);
        assertNotEquals(firstTBoolInstSet, thirdTBoolInstSet);
        assertNotEquals(secondTBoolInstSet, thirdTBoolInstSet);
        assertNotEquals(firstTBoolInstSet, new Object());
    }

    @Test
    void testStringArrayStringConstructors() throws SQLException {
        String value = "{true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02}";

        TBoolInstSet tBoolInstSet = new TBoolInstSet(value);
        String[] instants = new String[]{
            "true@2001-01-01 08:00:00+02",
            "false@2001-01-03 08:00:00+02"
        };
        TBoolInstSet otherTBoolInstSet = new TBoolInstSet(instants);

        assertEquals(tBoolInstSet.getValues(), otherTBoolInstSet.getValues());
    }

    @Test
    void testBoolSetType() throws SQLException {
        String value = "{{true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02}";
        TBoolInstSet tBoolInstSet = new TBoolInstSet(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tBoolInstSet.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        String value = String.format(
            "{true@2001-01-01 08:00:00%1$s, false@2001-01-03 08:00:00%1$s}",
            format.format(tz)
        );
        TBoolInstSet tBoolInstSet = new TBoolInstSet(value);
        String newValue = tBoolInstSet.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        List<Boolean> list = tBoolInstSet.getValues();
        assertEquals(3 , list.size());
        assertEquals(false , list.get(0));
        assertEquals(true , list.get(1));
        assertEquals(true , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertEquals(false, tBoolInstSet.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertEquals(true, tBoolInstSet.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertEquals(false, tBoolInstSet.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertEquals(true, tBoolInstSet.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertNull(tBoolInstSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertEquals(false, tBoolInstSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertEquals(3, tBoolInstSet.numTimestamps());
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
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertEquals(3, tBoolInstSet.timestamps().length);
        assertEquals(firstExpectedDate, tBoolInstSet.timestamps()[0]);
        assertEquals(secondExpectedDate, tBoolInstSet.timestamps()[1]);
        assertEquals(thirdExpectedDate, tBoolInstSet.timestamps()[2]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tBoolInstSet.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tBoolInstSet.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tBoolInstSet.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tBoolInstSet.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,true,true);
        assertEquals(period, tBoolInstSet.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
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
        assertEquals(periodSet, tBoolInstSet.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertEquals(3, tBoolInstSet.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        TBoolInst tBoolInst = new TBoolInst("false@2001-01-01 08:00:00+02");
        assertEquals(tBoolInst, tBoolInstSet.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        TBoolInst tBoolInst = new TBoolInst("true@2001-01-04 08:00:00+02");
        assertEquals(tBoolInst, tBoolInstSet.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        TBoolInst tBoolInst = new TBoolInst("true@2001-01-03 08:00:00+02");
        assertEquals(tBoolInst, tBoolInstSet.instantN(1));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tBoolInstSet.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        ArrayList<TBoolInst> list = new ArrayList<>();
        TBoolInst firstTBoolInst = new TBoolInst("false@2001-01-01 08:00:00+02");
        TBoolInst secondTBoolInst = new TBoolInst("true@2001-01-03 08:00:00+02");
        TBoolInst thirdTBoolInst = new TBoolInst("true@2001-01-04 08:00:00+02");
        list.add(firstTBoolInst);
        list.add(secondTBoolInst);
        list.add(thirdTBoolInst);
        assertEquals(3, list.size());
        assertEquals(list, tBoolInstSet.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertEquals(Duration.ZERO, tBoolInstSet.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tBoolInstSet.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        TBoolInstSet otherTBoolInstSet = new TBoolInstSet(
                "{false@2001-01-03 08:00:00+02, true@2001-01-05 08:00:00+02, true@2001-01-06 08:00:00+02}");
        tBoolInstSet.shift(Duration.ofDays(2));
        assertEquals(otherTBoolInstSet, tBoolInstSet);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        assertTrue(tBoolInstSet.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tBoolInstSet.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tBoolInstSet.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tBoolInstSet.intersectsPeriod(period));
    }

    @Test
    void testEmpty() {
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> {
                    TBoolInst[] values = new TBoolInst[0];
                    TBoolInstSet temporal = new TBoolInstSet(values);
                }
        );
        assertTrue(thrown.getMessage().contains(" must be composed of at least one instant."));
    }

    @Test
    void testInvalid() {
        String value = "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-01 08:00:00+02}";
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> {
                    TBoolInstSet temporal = new TBoolInstSet(value);
                }
        );
        assertTrue(thrown.getMessage().contains("The timestamps of a Temporal instant set must be increasing."));
    }
}
