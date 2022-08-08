package com.mobilitydb.jdbc.unit.ttext;

import com.mobilitydb.jdbc.tbool.TBoolInstSet;
import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.ttext.TTextInst;
import com.mobilitydb.jdbc.ttext.TTextInstSet;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TTextInstSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{Random@2001-01-01 08:00:00+02, Random2@2001-01-03 08:00:00+02}";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);

        TTextInstSet tTextInstSet = new TTextInstSet(value);
        TTextInst[] instants = new TTextInst[]{
                new TTextInst("Random", dateOne),
                new TTextInst("Random2", dateTwo)
        };
        TTextInstSet otherTIntInstSet = new TTextInstSet(instants);

        assertEquals(tTextInstSet.getValues(), otherTIntInstSet.getValues());
        assertEquals(tTextInstSet, otherTIntInstSet);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02}";
        String secondValue = "{jkl@2001-01-01 08:00:00+02, mno@2001-01-03 08:00:00+02}";
        String thirdValue = "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}";

        TTextInstSet firstTIntInstSet = new TTextInstSet(firstValue);
        TTextInstSet secondTIntInstSet = new TTextInstSet(secondValue);
        TTextInstSet thirdTIntInstSet = new TTextInstSet(thirdValue);

        assertNotEquals(firstTIntInstSet, secondTIntInstSet);
        assertNotEquals(firstTIntInstSet, thirdTIntInstSet);
        assertNotEquals(secondTIntInstSet, thirdTIntInstSet);
        assertNotEquals(firstTIntInstSet, new Object());
    }

    @Test
    void testStringArrayStringConstructors() throws SQLException {
        String value = "{ABCD@2001-01-01 08:00:00+02, JKLM@2001-01-03 08:00:00+02}";

        TTextInstSet tTextInstSet = new TTextInstSet(value);
        String[] instants = new String[]{
                "ABCD@2001-01-01 08:00:00+02",
                "JKLM@2001-01-03 08:00:00+02"
        };
        TTextInstSet otherTTextInstSet = new TTextInstSet(instants);

        assertEquals(tTextInstSet.getValues(), otherTTextInstSet.getValues());
    }

    @Test
    void testInstSetType() throws SQLException {
        String value = "{Test@2001-01-01 08:00:00+02, Other@2001-01-03 08:00:00+02}";
        TTextInstSet tTextInstSet = new TTextInstSet(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tTextInstSet.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        String value = String.format(
                "{this@2001-01-01 08:00:00%1$s, that@2001-01-03 08:00:00%1$s}",
                format.format(tz)
        );
        TTextInstSet tIntItTextInstSetstSet = new TTextInstSet(value);
        String newValue = tIntItTextInstSetstSet.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        List<String> list = tTextInstSet.getValues();
        assertEquals(3 , list.size());
        assertEquals("pqr" , list.get(0));
        assertEquals("stu" , list.get(1));
        assertEquals("vwx" , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        assertEquals("pqr", tTextInstSet.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        assertEquals("vwx", tTextInstSet.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{abc@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        assertEquals("abc", tTextInstSet.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, jkl@2001-01-04 08:00:00+02}");
        assertEquals("stu", tTextInstSet.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        assertNull(tTextInstSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        assertEquals("pqr", tTextInstSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        assertEquals(3, tTextInstSet.numTimestamps());
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
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        assertEquals(3, tTextInstSet.timestamps().length);
        assertEquals(firstExpectedDate, tTextInstSet.timestamps()[0]);
        assertEquals(secondExpectedDate, tTextInstSet.timestamps()[1]);
        assertEquals(thirdExpectedDate, tTextInstSet.timestamps()[2]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tTextInstSet.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tTextInstSet.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tTextInstSet.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        assertEquals(expectedDate, tTextInstSet.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,true,true);
        assertEquals(period, tTextInstSet.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
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
        assertEquals(periodSet, tTextInstSet.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        assertEquals(3, tTextInstSet.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        TTextInst tTextInst = new TTextInst("pqr@2001-01-01 08:00:00+02");
        assertEquals(tTextInst, tTextInstSet.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        TTextInst tTextInst = new TTextInst("vwx@2001-01-04 08:00:00+02");
        assertEquals(tTextInst, tTextInstSet.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        TTextInst tTextInst = new TTextInst("stu@2001-01-03 08:00:00+02");
        assertEquals(tTextInst, tTextInstSet.instantN(1));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tTextInstSet.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        ArrayList<TTextInst> list = new ArrayList<>();
        TTextInst firstTTextInst = new TTextInst("pqr@2001-01-01 08:00:00+02");
        TTextInst secondTTextInst = new TTextInst("stu@2001-01-03 08:00:00+02");
        TTextInst thirdTTextInst = new TTextInst("vwx@2001-01-04 08:00:00+02");
        list.add(firstTTextInst);
        list.add(secondTTextInst);
        list.add(thirdTTextInst);
        assertEquals(3, list.size());
        assertEquals(list, tTextInstSet.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        assertEquals(Duration.ZERO, tTextInstSet.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tTextInstSet.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        TTextInstSet otherTTextInstSet = new TTextInstSet(
                "{pqr@2001-01-03 08:00:00+02, stu@2001-01-05 08:00:00+02, vwx@2001-01-06 08:00:00+02}");
        tTextInstSet.shift(Duration.ofDays(2));
        assertEquals(otherTTextInstSet, tTextInstSet);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        assertTrue(tTextInstSet.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tTextInstSet.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tTextInstSet.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tTextInstSet.intersectsPeriod(period));
    }
}
