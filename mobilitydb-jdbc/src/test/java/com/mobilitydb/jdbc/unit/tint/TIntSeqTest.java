package com.mobilitydb.jdbc.unit.tint;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.tint.TIntInst;
import com.mobilitydb.jdbc.tint.TIntSeq;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TIntSeqTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "[2@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02)";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TIntInst[] instants = new TIntInst[]{
                new TIntInst(2, dateOne),
                new TIntInst(2, dateTwo)
        };
        String[] stringInstants = new String[]{
                "2@2001-01-01 08:00:00+02",
                "2@2001-01-03 08:00:00+02"
        };

        TIntSeq firstTemporal = new TIntSeq(value);
        TIntSeq secondTemporal = new TIntSeq(instants);
        TIntSeq thirdTemporal = new TIntSeq(stringInstants);
        TIntSeq fourthTemporal = new TIntSeq(instants, true, false);
        TIntSeq fifthTemporal = new TIntSeq(stringInstants, true, false);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
        assertEquals(firstTemporal, fourthTemporal);
        assertEquals(firstTemporal, fifthTemporal);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "(1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02]";
        String secondValue = "(1@2001-01-01 08:00:00+02, 2@2001-01-06 08:00:00+02]";
        String thirdValue = "[1@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)";

        TIntSeq firstTemporal = new TIntSeq(firstValue);
        TIntSeq secondTemporal = new TIntSeq(secondValue);
        TIntSeq thirdTemporal = new TIntSeq(thirdValue);

        assertNotEquals(firstTemporal, secondTemporal);
        assertNotEquals(firstTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, thirdTemporal);
        assertNotEquals(firstTemporal, new Object());
    }

    @Test
    void testSeqType() throws SQLException {
        String value = "(1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02]";
        TIntSeq temporal = new TIntSeq(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE, temporal.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        String value = String.format(
                "(1@2001-01-01 08:00:00%1$s, 2@2001-01-03 08:00:00%1$s]",
                format.format(tz)
        );
        TIntSeq temporal = new TIntSeq(value);
        String newValue = temporal.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "(1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02]");
        List<Integer> list = tIntSeq.getValues();
        assertEquals(3 , list.size());
        assertEquals(1 , list.get(0));
        assertEquals(2 , list.get(1));
        assertEquals(3 , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[1@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        assertEquals(1, tIntSeq.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[1@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        assertEquals(3, tIntSeq.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "(14@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02]");
        assertEquals(2, tIntSeq.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "(15@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02]");
        assertEquals(15, tIntSeq.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TIntSeq tIntSeq = new TIntSeq(
                "[1@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        assertNull(tIntSeq.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        assertEquals(89, tIntSeq.valueAtTimestamp(timestamp));
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        assertEquals(3, tIntSeq.numTimestamps());
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
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        assertEquals(3, tIntSeq.timestamps().length);
        assertEquals(firstExpectedDate, tIntSeq.timestamps()[0]);
        assertEquals(secondExpectedDate, tIntSeq.timestamps()[1]);
        assertEquals(thirdExpectedDate, tIntSeq.timestamps()[2]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        assertEquals(expectedDate, tIntSeq.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tIntSeq.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        assertEquals(expectedDate, tIntSeq.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        assertEquals(expectedDate, tIntSeq.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,true,false);
        assertEquals(period, tIntSeq.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialExpectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endExpectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);

        Period period = new Period(initialExpectedDate,endExpectedDate,true,false);
        PeriodSet periodSet = new PeriodSet(period);
        assertEquals(periodSet, tIntSeq.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        assertEquals(3, tIntSeq.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        TIntInst tIntInst = new TIntInst("89@2001-01-01 08:00:00+02");
        assertEquals(tIntInst, tIntSeq.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        TIntInst tIntInst = new TIntInst("3@2001-01-04 08:00:00+02");
        assertEquals(tIntInst, tIntSeq.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "(89@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02]");
        TIntInst tIntInst = new TIntInst("2@2001-01-03 08:00:00+02");
        assertEquals(tIntInst, tIntSeq.instantN(1));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tIntSeq.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        ArrayList<TIntInst> list = new ArrayList<>();
        TIntInst firstTIntInst = new TIntInst("89@2001-01-01 08:00:00+02");
        TIntInst secondTIntInst = new TIntInst("3@2001-01-03 08:00:00+02");
        TIntInst thirdTIntInst = new TIntInst("3@2001-01-04 08:00:00+02");
        list.add(firstTIntInst);
        list.add(secondTIntInst);
        list.add(thirdTIntInst);
        assertEquals(3, list.size());
        assertEquals(list, tIntSeq.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tIntSeq.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tIntSeq.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        TIntSeq otherTIntSeq = new TIntSeq(
                "[89@2001-01-03 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02)");
        tIntSeq.shift(Duration.ofDays(2));
        assertEquals(otherTIntSeq, tIntSeq);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        assertTrue(tIntSeq.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tIntSeq.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tIntSeq.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tIntSeq.intersectsPeriod(period));
    }

    @Test
    void testNumSequences() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        assertEquals(1, tIntSeq.numSequences());
    }

    @Test
    void testStartSequence() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        assertEquals(tIntSeq, tIntSeq.startSequence());
    }

    @Test
    void testEndSequence() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        assertEquals(tIntSeq, tIntSeq.endSequence());
    }

    @Test
    void testSequenceN() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        assertEquals(tIntSeq, tIntSeq.sequenceN(0));
    }

    @Test
    void testSequenceNNoValue() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tIntSeq.sequenceN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no sequence at this index."));
    }

    @Test
    void testSequences() throws SQLException {
        TIntSeq tIntSeq = new TIntSeq(
                "[89@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02)");
        ArrayList<TIntSeq> list = new ArrayList<>();
        list.add(tIntSeq);
        assertEquals(list, tIntSeq.sequences());
    }
}
