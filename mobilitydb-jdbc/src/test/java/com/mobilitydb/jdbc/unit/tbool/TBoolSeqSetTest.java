package com.mobilitydb.jdbc.unit.tbool;

import com.mobilitydb.jdbc.tbool.TBoolInst;
import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tbool.TBoolSeq;
import com.mobilitydb.jdbc.tbool.TBoolSeqSet;
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

class TBoolSeqSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{(false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02], " +
                "[false@2001-01-04 08:00:00+02, true@2001-01-05 08:00:00+02, true@2001-01-06 08:00:00+02]}";

        TBoolSeq[] sequences = new TBoolSeq[]{
            new TBoolSeq("(false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02]"),
            new TBoolSeq("[false@2001-01-04 08:00:00+02, true@2001-01-05 08:00:00+02, true@2001-01-06 08:00:00+02]")
        };
        String[] stringSequences = new String[]{
            "(false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02]",
            "[false@2001-01-04 08:00:00+02, true@2001-01-05 08:00:00+02, true@2001-01-06 08:00:00+02]"
        };

        TBoolSeqSet firstTemporal = new TBoolSeqSet(value);
        TBoolSeqSet secondTemporal = new TBoolSeqSet(sequences);
        TBoolSeqSet thirdTemporal = new TBoolSeqSet(stringSequences);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{(true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02], " +
                "[true@2001-01-04 08:00:00+02, true@2001-01-05 08:00:00+02, true@2001-01-06 08:00:00+02]}";
        String secondValue = "{[false@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02)}";
        String thirdValue = "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                "(true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}";

        TBoolSeqSet firstTemporal = new TBoolSeqSet(firstValue);
        TBoolSeqSet secondTemporal = new TBoolSeqSet(secondValue);
        TBoolSeqSet thirdTemporal = new TBoolSeqSet(thirdValue);

        assertNotEquals(firstTemporal, secondTemporal);
        assertNotEquals(firstTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, thirdTemporal);
        assertNotEquals(firstTemporal, new Object());
    }

    @Test
    void testBoolSeqSetType() throws SQLException {
        String value = "{(false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02], " +
                "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02, true@2001-01-06 08:00:00+02]}";
        TBoolSeqSet temporal = new TBoolSeqSet(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE_SET, temporal.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        String value = String.format(
            "{[true@2001-01-01 08:00:00%1$s, true@2001-01-03 08:00:00%1$s), " +
                "[false@2001-01-04 08:00:00%1$s, false@2001-01-05 08:00:00%1$s, true@2001-01-06 08:00:00%1$s]}",
            format.format(tz)
        );
        TBoolSeqSet temporal = new TBoolSeqSet(value);
        String newValue = temporal.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        List<Boolean> list = tBoolSeqSet.getValues();
        assertEquals(4 , list.size());
        assertEquals(true , list.get(0));
        assertEquals(true , list.get(1));
        assertEquals(true , list.get(2));
        assertEquals(false , list.get(3));
    }

    @Test
    void testStartValue() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        assertEquals(true, tBoolSeqSet.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        assertEquals(false, tBoolSeqSet.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        assertEquals(false, tBoolSeqSet.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        assertEquals(true, tBoolSeqSet.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        assertNull(tBoolSeqSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        assertEquals(true, tBoolSeqSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        assertEquals(4, tBoolSeqSet.numTimestamps());
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
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        assertEquals(4, tBoolSeqSet.timestamps().length);
        assertEquals(firstExpectedDate, tBoolSeqSet.timestamps()[0]);
        assertEquals(secondExpectedDate, tBoolSeqSet.timestamps()[1]);
        assertEquals(thirdExpectedDate, tBoolSeqSet.timestamps()[2]);
        assertEquals(fourthExpectedDate, tBoolSeqSet.timestamps()[3]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        assertEquals(expectedDate, tBoolSeqSet.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tBoolSeqSet.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        assertEquals(expectedDate, tBoolSeqSet.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 5,
                8, 0, 0, 0, tz);
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        assertEquals(expectedDate, tBoolSeqSet.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 5,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,true,true);
        assertEquals(period, tBoolSeqSet.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialExpectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endExpectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        Period firstPeriod = new Period(initialExpectedDate,endExpectedDate,true,false);
        initialExpectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        endExpectedDate = OffsetDateTime.of(2001,1, 5,
                8, 0, 0, 0, tz);
        Period secondPeriod = new Period(initialExpectedDate,endExpectedDate,true,true);
        PeriodSet periodSet = new PeriodSet(firstPeriod, secondPeriod);
        assertEquals(periodSet, tBoolSeqSet.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        assertEquals(4, tBoolSeqSet.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        TBoolInst tBoolInst = new TBoolInst("true@2001-01-01 08:00:00+02");
        assertEquals(tBoolInst, tBoolSeqSet.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        TBoolInst tBoolInst = new TBoolInst("false@2001-01-05 08:00:00+02");
        assertEquals(tBoolInst, tBoolSeqSet.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        TBoolInst tBoolInst = new TBoolInst("true@2001-01-03 08:00:00+02");
        assertEquals(tBoolInst, tBoolSeqSet.instantN(1));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tBoolSeqSet.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        ArrayList<TBoolInst> list = new ArrayList<>();
        TBoolInst firstTBoolInst = new TBoolInst("true@2001-01-01 08:00:00+02");
        TBoolInst secondTBoolInst = new TBoolInst("true@2001-01-03 08:00:00+02");
        TBoolInst thirdTBoolInst = new TBoolInst("true@2001-01-04 08:00:00+02");
        TBoolInst fourthTBoolInst = new TBoolInst("false@2001-01-05 08:00:00+02");
        list.add(firstTBoolInst);
        list.add(secondTBoolInst);
        list.add(thirdTBoolInst);
        list.add(fourthTBoolInst);
        assertEquals(4, list.size());
        assertEquals(list, tBoolSeqSet.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        initialDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        endDate = OffsetDateTime.of(2001,1, 5,
                8, 0, 0, 0, tz);
        expectedDuration = Duration.between(initialDate, endDate).plus(expectedDuration);
        assertEquals(expectedDuration, tBoolSeqSet.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 5,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tBoolSeqSet.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        TBoolSeqSet otherTBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-03 08:00:00+02, true@2001-01-05 08:00:00+02), " +
                        "[true@2001-01-06 08:00:00+02, false@2001-01-07 08:00:00+02]}");
        tBoolSeqSet.shift(Duration.ofDays(2));
        assertEquals(otherTBoolSeqSet, tBoolSeqSet);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        assertTrue(tBoolSeqSet.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tBoolSeqSet.intersectsTimestamp(date));
    }

   @Test
    void testIntersectsPeriod() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tBoolSeqSet.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tBoolSeqSet.intersectsPeriod(period));
    }

    @Test
    void testNumSequences() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        assertEquals(2, tBoolSeqSet.numSequences());
    }

    @Test
    void testStartSequence() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        TBoolSeq tBoolSeq = new TBoolSeq("[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02)");
        assertEquals(tBoolSeq, tBoolSeqSet.startSequence());
    }

    @Test
    void testEndSequence() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        TBoolSeq tBoolSeq = new TBoolSeq("[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]");
        assertEquals(tBoolSeq, tBoolSeq.endSequence());
    }

    @Test
    void testSequenceN() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        TBoolSeq tBoolSeq = new TBoolSeq("[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]");
        assertEquals(tBoolSeq, tBoolSeqSet.sequenceN(1));
    }

    @Test
    void testSequenceNNoValue() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tBoolSeqSet.sequenceN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no sequence at this index."));
    }

    @Test
    void testSequences() throws SQLException {
        TBoolSeqSet tBoolSeqSet = new TBoolSeqSet(
                "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                        "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]}");
        ArrayList<TBoolSeq> list = new ArrayList<>();
        list.add(new TBoolSeq("[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02)"));
        list.add(new TBoolSeq("[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02]"));
        assertEquals(list, tBoolSeqSet.sequences());
    }

    @Test
    void testEmpty() {
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> {
                    TBoolSeq[] values = new TBoolSeq[0];
                    TBoolSeqSet temporal = new TBoolSeqSet(values);
                }
        );
        assertTrue(thrown.getMessage().contains("Sequence set must be composed of at least one sequence."));
    }

    @Test
    void testOverlap() {
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> {
                    TBoolSeqSet temporal = new TBoolSeqSet(
                            "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                                    "[true@2001-01-02 08:00:00+02, false@2001-01-05 08:00:00+02]}");
                }
        );
        assertTrue(thrown.getMessage().contains("The sequences of a sequence set cannot overlap."));
    }
}
