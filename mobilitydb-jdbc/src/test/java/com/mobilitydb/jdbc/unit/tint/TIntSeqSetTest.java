package com.mobilitydb.jdbc.unit.tint;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.tint.TIntInst;
import com.mobilitydb.jdbc.tint.TIntSeq;
import com.mobilitydb.jdbc.tint.TIntSeqSet;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TIntSeqSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{[1@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02), " +
                "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}";

        TIntSeq[] sequences = new TIntSeq[]{
            new TIntSeq("[1@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02)"),
            new TIntSeq("[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]")
        };
        String[] stringSequences = new String[]{
            "[1@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02)",
            "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]"
        };

        TIntSeqSet firstTemporal = new TIntSeqSet(value);
        TIntSeqSet secondTemporal = new TIntSeqSet(sequences);
        TIntSeqSet thirdTemporal = new TIntSeqSet(stringSequences);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{[1@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02), " +
                "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}";
        String secondValue = "{[1@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02)}";
        String thirdValue = "{[1@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02), " +
                "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02]}";

        TIntSeqSet firstTemporal = new TIntSeqSet(firstValue);
        TIntSeqSet secondTemporal = new TIntSeqSet(secondValue);
        TIntSeqSet thirdTemporal = new TIntSeqSet(thirdValue);

        assertNotEquals(firstTemporal, secondTemporal);
        assertNotEquals(firstTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, thirdTemporal);
        assertNotEquals(firstTemporal, new Object());
    }

    @Test
    void testSeqSetType() throws SQLException {
        String value = "{[1@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02), " +
                "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}";
        TIntSeqSet temporal = new TIntSeqSet(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE_SET, temporal.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        String value = String.format(
            "{[1@2001-01-01 08:00:00%1$s, 1@2001-01-03 08:00:00%1$s), " +
                "[2@2001-01-04 08:00:00%1$s, 3@2001-01-05 08:00:00%1$s, 3@2001-01-06 08:00:00%1$s]}",
            format.format(tz)
        );
        TIntSeqSet temporal = new TIntSeqSet(value);
        String newValue = temporal.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{[1@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02), " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        List<Integer> list = tIntSeqSet.getValues();
        assertEquals(5 , list.size());
        assertEquals(1 , list.get(0));
        assertEquals(1 , list.get(1));
        assertEquals(2 , list.get(2));
        assertEquals(3 , list.get(3));
        assertEquals(3 , list.get(4));
    }

    @Test
    void testStartValue() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{[1@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02), " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        assertEquals(1, tIntSeqSet.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{[1@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02), " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        assertEquals(3, tIntSeqSet.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{[1@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02), " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        assertEquals(1, tIntSeqSet.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{[1@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02), " +
                        "[2@2001-01-04 08:00:00+02, 9@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        assertEquals(9, tIntSeqSet.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{[1@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02), " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        assertNull(tIntSeqSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        assertEquals(189, tIntSeqSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        assertEquals(5, tIntSeqSet.numTimestamps());
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
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        assertEquals(5, tIntSeqSet.timestamps().length);
        assertEquals(firstExpectedDate, tIntSeqSet.timestamps()[0]);
        assertEquals(secondExpectedDate, tIntSeqSet.timestamps()[1]);
        assertEquals(thirdExpectedDate, tIntSeqSet.timestamps()[2]);
        assertEquals(fourthExpectedDate, tIntSeqSet.timestamps()[3]);
        assertEquals(fifthExpectedDate, tIntSeqSet.timestamps()[4]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        assertEquals(expectedDate, tIntSeqSet.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tIntSeqSet.timestampN(6)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        assertEquals(expectedDate, tIntSeqSet.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        assertEquals(expectedDate, tIntSeqSet.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,false,true);
        assertEquals(period, tIntSeqSet.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
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
        Period secondPeriod = new Period(initialExpectedDate, endExpectedDate,true,true);
        PeriodSet periodSet = new PeriodSet(firstPeriod, secondPeriod);
        assertEquals(periodSet, tIntSeqSet.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        assertEquals(5, tIntSeqSet.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        TIntInst tIntInst = new TIntInst("189@2001-01-01 08:00:00+02");
        assertEquals(tIntInst, tIntSeqSet.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        TIntInst tIntInst = new TIntInst("3@2001-01-06 08:00:00+02");
        assertEquals(tIntInst, tIntSeqSet.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        TIntInst tIntInst = new TIntInst("3@2001-01-05 08:00:00+02");
        assertEquals(tIntInst, tIntSeqSet.instantN(3));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tIntSeqSet.instantN(6)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        ArrayList<TIntInst> list = new ArrayList<>();
        TIntInst firstInst = new TIntInst("189@2001-01-01 08:00:00+02");
        TIntInst secondInst = new TIntInst("1@2001-01-03 08:00:00+02");
        TIntInst thirdTInst = new TIntInst("2@2001-01-04 08:00:00+02");
        TIntInst fourthInst = new TIntInst("3@2001-01-05 08:00:00+02");
        TIntInst fifthInst = new TIntInst("3@2001-01-06 08:00:00+02");
        list.add(firstInst);
        list.add(secondInst);
        list.add(thirdTInst);
        list.add(fourthInst);
        list.add(fifthInst);
        assertEquals(5, list.size());
        assertEquals(list, tIntSeqSet.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
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
        assertEquals(expectedDuration, tIntSeqSet.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tIntSeqSet.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        TIntSeqSet otherTIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-03 08:00:00+02, 1@2001-01-05 08:00:00+02], " +
                        "[2@2001-01-06 08:00:00+02, 3@2001-01-07 08:00:00+02, 3@2001-01-08 08:00:00+02]}");
        tIntSeqSet.shift(Duration.ofDays(2));
        assertEquals(otherTIntSeqSet, tIntSeqSet);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{[1@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02), " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        assertTrue(tIntSeqSet.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tIntSeqSet.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tIntSeqSet.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tIntSeqSet.intersectsPeriod(period));
    }

    @Test
    void testNumSequences() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        assertEquals(2, tIntSeqSet.numSequences());
    }

    @Test
    void testStartSequence() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        TIntSeq tIntSeq = new TIntSeq("(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02]");
        assertEquals(tIntSeq, tIntSeqSet.startSequence());
    }

    @Test
    void testEndSequence() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        TIntSeq tIntSeq = new TIntSeq(
                "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]");
        assertEquals(tIntSeq, tIntSeqSet.endSequence());
    }

    @Test
    void testSequenceN() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        TIntSeq tIntSeq = new TIntSeq(
                "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]");
        assertEquals(tIntSeq, tIntSeqSet.sequenceN(1));
    }

    @Test
    void testSequenceNNoValue() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tIntSeqSet.sequenceN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no sequence at this index."));
    }

    @Test
    void testSequences() throws SQLException {
        TIntSeqSet tIntSeqSet = new TIntSeqSet(
                "{(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02], " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        ArrayList<TIntSeq> list = new ArrayList<>();
        list.add(new TIntSeq(
                "(189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02]"));
        list.add(new TIntSeq(
                "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]"));
        assertEquals(list, tIntSeqSet.sequences());
    }
}
