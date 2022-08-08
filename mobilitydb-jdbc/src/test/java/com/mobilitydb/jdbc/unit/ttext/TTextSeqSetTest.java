package com.mobilitydb.jdbc.unit.ttext;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.ttext.TTextInst;
import com.mobilitydb.jdbc.ttext.TTextSeq;
import com.mobilitydb.jdbc.ttext.TTextSeqSet;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TTextSeqSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{(A@2001-01-01 08:00:00+02, B@2001-01-03 08:00:00+02], " +
                "[C@2001-01-04 08:00:00+02, D@2001-01-05 08:00:00+02, E@2001-01-06 08:00:00+02]}";

        TTextSeq[] sequences = new TTextSeq[]{
                new TTextSeq("(A@2001-01-01 08:00:00+02, B@2001-01-03 08:00:00+02]"),
                new TTextSeq("[C@2001-01-04 08:00:00+02, D@2001-01-05 08:00:00+02, E@2001-01-06 08:00:00+02]")
        };
        String[] stringSequences = new String[]{
                "(A@2001-01-01 08:00:00+02, B@2001-01-03 08:00:00+02]",
                "[C@2001-01-04 08:00:00+02, D@2001-01-05 08:00:00+02, E@2001-01-06 08:00:00+02]"
        };

        TTextSeqSet firstTemporal = new TTextSeqSet(value);
        TTextSeqSet secondTemporal = new TTextSeqSet(sequences);
        TTextSeqSet thirdTemporal = new TTextSeqSet(stringSequences);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{(aaa@2001-01-01 08:00:00+02, bbb@2001-01-03 08:00:00+02], " +
                "[ccc@2001-01-04 08:00:00+02, ddd@2001-01-05 08:00:00+02, eee@2001-01-06 08:00:00+02]}";
        String secondValue = "{[aaa@2001-01-01 08:00:00+02, aaa@2001-01-03 08:00:00+02)}";
        String thirdValue = "{[aaa@2001-01-01 08:00:00+02, aaa@2001-01-03 08:00:00+02), " +
                "[ccc@2001-01-04 08:00:00+02, ddd@2001-01-05 08:00:00+02]}";

        TTextSeqSet firstTemporal = new TTextSeqSet(firstValue);
        TTextSeqSet secondTemporal = new TTextSeqSet(secondValue);
        TTextSeqSet thirdTemporal = new TTextSeqSet(thirdValue);

        assertNotEquals(firstTemporal, secondTemporal);
        assertNotEquals(firstTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, thirdTemporal);
        assertNotEquals(firstTemporal, new Object());
    }

    @Test
    void testSeqSetType() throws SQLException {
        String value = "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}";
        TTextSeqSet temporal = new TTextSeqSet(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE_SET, temporal.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        String value = String.format(
                "{(abc@2001-01-01 08:00:00%1$s, def@2001-01-03 08:00:00%1$s], " +
                        "[ghi@2001-01-04 08:00:00%1$s, jkl@2001-01-05 08:00:00%1$s, mno@2001-01-06 08:00:00%1$s]}",
                format.format(tz)
        );
        TTextSeqSet temporal = new TTextSeqSet(value);
        String newValue = temporal.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        List<String> list = tTextSeqSet.getValues();
        assertEquals(5 , list.size());
        assertEquals("abc" , list.get(0));
        assertEquals("def" , list.get(1));
        assertEquals("ghi" , list.get(2));
        assertEquals("jkl" , list.get(3));
        assertEquals("mno" , list.get(4));
    }

    @Test
    void testStartValue() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertEquals("abc", tTextSeqSet.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertEquals("mno", tTextSeqSet.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, aaa@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertEquals("aaa", tTextSeqSet.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, zzz@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertEquals("zzz", tTextSeqSet.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertNull(tTextSeqSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertEquals("ghi", tTextSeqSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertEquals(5, tTextSeqSet.numTimestamps());
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
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertEquals(5, tTextSeqSet.timestamps().length);
        assertEquals(firstExpectedDate, tTextSeqSet.timestamps()[0]);
        assertEquals(secondExpectedDate, tTextSeqSet.timestamps()[1]);
        assertEquals(thirdExpectedDate, tTextSeqSet.timestamps()[2]);
        assertEquals(fourthExpectedDate, tTextSeqSet.timestamps()[3]);
        assertEquals(fifthExpectedDate, tTextSeqSet.timestamps()[4]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertEquals(expectedDate, tTextSeqSet.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tTextSeqSet.timestampN(6)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertEquals(expectedDate, tTextSeqSet.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertEquals(expectedDate, tTextSeqSet.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,false,true);
        assertEquals(period, tTextSeqSet.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialExpectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endExpectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        Period firstPeriod = new Period(initialExpectedDate,endExpectedDate,false,true);
        initialExpectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        endExpectedDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        Period secondPeriod = new Period(initialExpectedDate,endExpectedDate,true,true);
        PeriodSet periodSet = new PeriodSet(firstPeriod, secondPeriod);
        assertEquals(periodSet, tTextSeqSet.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertEquals(5, tTextSeqSet.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        TTextInst tTextInst = new TTextInst("abc@2001-01-01 08:00:00+02");
        assertEquals(tTextInst, tTextSeqSet.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        TTextInst tTextInst = new TTextInst("mno@2001-01-06 08:00:00+02");
        assertEquals(tTextInst, tTextSeqSet.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        TTextInst tTextInst = new TTextInst(("jkl@2001-01-05 08:00:00+02"));
        assertEquals(tTextInst, tTextSeqSet.instantN(3));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tTextSeqSet.instantN(6)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        ArrayList<TTextInst> list = new ArrayList<>();
        TTextInst firstInst = new TTextInst("abc@2001-01-01 08:00:00+02");
        TTextInst secondInst = new TTextInst("def@2001-01-03 08:00:00+02");
        TTextInst thirdTInst = new TTextInst("ghi@2001-01-04 08:00:00+02");
        TTextInst fourthInst = new TTextInst("jkl@2001-01-05 08:00:00+02");
        TTextInst fifthInst = new TTextInst("mno@2001-01-06 08:00:00+02");
        list.add(firstInst);
        list.add(secondInst);
        list.add(thirdTInst);
        list.add(fourthInst);
        list.add(fifthInst);
        assertEquals(5, list.size());
        assertEquals(list, tTextSeqSet.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
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
        assertEquals(expectedDuration, tTextSeqSet.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tTextSeqSet.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        TTextSeqSet otherTTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-03 08:00:00+02, def@2001-01-05 08:00:00+02], " +
                        "[ghi@2001-01-06 08:00:00+02, jkl@2001-01-07 08:00:00+02, mno@2001-01-08 08:00:00+02]}");
        tTextSeqSet.shift(Duration.ofDays(2));
        assertEquals(otherTTextSeqSet, tTextSeqSet);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{[abc@2001-01-01 08:00:00+02, abc@2001-01-03 08:00:00+02), " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        assertTrue(tTextSeqSet.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tTextSeqSet.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tTextSeqSet.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tTextSeqSet.intersectsPeriod(period));
    }

    @Test
    void testNumSequences() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertEquals(2, tTextSeqSet.numSequences());
    }

    @Test
    void testStartSequence() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        TTextSeq tTextSeq = new TTextSeq("(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02]");
        assertEquals(tTextSeq, tTextSeqSet.startSequence());
    }

    @Test
    void testEndSequence() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        TTextSeq tTextSeq = new TTextSeq(
                "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]");
        assertEquals(tTextSeq, tTextSeqSet.endSequence());
    }

    @Test
    void testSequenceN() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        TTextSeq tTextSeq = new TTextSeq(
                "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]");
        assertEquals(tTextSeq, tTextSeqSet.sequenceN(1));
    }

    @Test
    void testSequenceNNoValue() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tTextSeqSet.sequenceN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no sequence at this index."));
    }

    @Test
    void testSequences() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02], " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        ArrayList<TTextSeq> list = new ArrayList<>();
        list.add(new TTextSeq(
                "(abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02]"));
        list.add(new TTextSeq(
                "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]"));
        assertEquals(list, tTextSeqSet.sequences());
    }
}
