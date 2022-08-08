package com.mobilitydb.jdbc.unit.tint;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.tint.TIntInst;
import com.mobilitydb.jdbc.tint.TIntInstSet;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TIntInstSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02}";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);

        TIntInstSet tIntInstSet = new TIntInstSet(value);
        TIntInst[] instants = new TIntInst[]{
            new TIntInst(1, dateOne),
            new TIntInst(2, dateTwo)
        };
        TIntInstSet otherTIntInstSet = new TIntInstSet(instants);

        assertEquals(tIntInstSet.getValues(), otherTIntInstSet.getValues());
        assertEquals(tIntInstSet, otherTIntInstSet);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02}";
        String secondValue = "{2@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02}";
        String thirdValue = "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}";

        TIntInstSet firstTIntInstSet = new TIntInstSet(firstValue);
        TIntInstSet secondTIntInstSet = new TIntInstSet(secondValue);
        TIntInstSet thirdTIntInstSet = new TIntInstSet(thirdValue);

        assertNotEquals(firstTIntInstSet, secondTIntInstSet);
        assertNotEquals(firstTIntInstSet, thirdTIntInstSet);
        assertNotEquals(secondTIntInstSet, thirdTIntInstSet);
        assertNotEquals(firstTIntInstSet, new Object());
    }

    @Test
    void testStringArrayStringConstructors() throws SQLException {
        String value = "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02}";

        TIntInstSet tIntInstSet = new TIntInstSet(value);
        String[] instants = new String[]{
            "1@2001-01-01 08:00:00+02",
            "2@2001-01-03 08:00:00+02"
        };
        TIntInstSet otherTIntInstSet = new TIntInstSet(instants);

        assertEquals(tIntInstSet.getValues(), otherTIntInstSet.getValues());
    }

    @Test
    void testInstSetType() throws SQLException {
        String value = "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02}";
        TIntInstSet tIntInstSet = new TIntInstSet(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tIntInstSet.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        String value = String.format(
            "{1@2001-01-01 08:00:00%1$s, 2@2001-01-03 08:00:00%1$s}",
            format.format(tz)
        );
        TIntInstSet tIntInstSet = new TIntInstSet(value);
        String newValue = tIntInstSet.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        List<Integer> list = tIntInstSet.getValues();
        assertEquals(3 , list.size());
        assertEquals(1 , list.get(0));
        assertEquals(2 , list.get(1));
        assertEquals(3 , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{8@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        assertEquals(8, tIntInstSet.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        assertEquals(3, tIntInstSet.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{15@2001-01-01 08:00:00+02, 87@2001-01-03 08:00:00+02, 43@2001-01-04 08:00:00+02}");
        assertEquals(15, tIntInstSet.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{178@2001-01-01 08:00:00+02, 252@2001-01-03 08:00:00+02, 43@2001-01-04 08:00:00+02}");
        assertEquals(252, tIntInstSet.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        assertNull(tIntInstSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        assertEquals(18, tIntInstSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        assertEquals(3, tIntInstSet.numTimestamps());
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
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        assertEquals(3, tIntInstSet.timestamps().length);
        assertEquals(firstExpectedDate, tIntInstSet.timestamps()[0]);
        assertEquals(secondExpectedDate, tIntInstSet.timestamps()[1]);
        assertEquals(thirdExpectedDate, tIntInstSet.timestamps()[2]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tIntInstSet.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tIntInstSet.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tIntInstSet.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tIntInstSet.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,true,true);
        assertEquals(period, tIntInstSet.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
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
        assertEquals(periodSet, tIntInstSet.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        assertEquals(3, tIntInstSet.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        TIntInst tIntInst = new TIntInst("18@2001-01-01 08:00:00+02");
        assertEquals(tIntInst, tIntInstSet.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        TIntInst tIntInst = new TIntInst("3@2001-01-04 08:00:00+02");
        assertEquals(tIntInst, tIntInstSet.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        TIntInst tIntInst = new TIntInst("2@2001-01-03 08:00:00+02");
        assertEquals(tIntInst, tIntInstSet.instantN(1));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tIntInstSet.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        ArrayList<TIntInst> list = new ArrayList<>();
        TIntInst firstTIntInst= new TIntInst("18@2001-01-01 08:00:00+02");
        TIntInst secondTIntInst = new TIntInst("2@2001-01-03 08:00:00+02");
        TIntInst thirdTIntInst = new TIntInst("3@2001-01-04 08:00:00+02");
        list.add(firstTIntInst);
        list.add(secondTIntInst);
        list.add(thirdTIntInst);
        assertEquals(3, list.size());
        assertEquals(list, tIntInstSet.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        assertEquals(Duration.ZERO, tIntInstSet.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tIntInstSet.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        TIntInstSet otherTIntInstSet = new TIntInstSet(
                "{18@2001-01-03 08:00:00+02, 2@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02}");
        tIntInstSet.shift(Duration.ofDays(2));
        assertEquals(otherTIntInstSet, tIntInstSet);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        assertTrue(tIntInstSet.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tIntInstSet.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tIntInstSet.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tIntInstSet.intersectsPeriod(period));
    }
}
