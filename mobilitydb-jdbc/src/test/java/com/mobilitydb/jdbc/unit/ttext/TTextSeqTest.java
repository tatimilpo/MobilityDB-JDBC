package com.mobilitydb.jdbc.unit.ttext;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.ttext.TTextInst;
import com.mobilitydb.jdbc.ttext.TTextSeq;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TTextSeqTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "[This is a text@2001-01-01 08:00:00+02, This is a text@2001-01-03 08:00:00+02)";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TTextInst[] instants = new TTextInst[]{
                new TTextInst("This is a text", dateOne),
                new TTextInst("This is a text", dateTwo)
        };
        String[] stringInstants = new String[]{
                "This is a text@2001-01-01 08:00:00+02",
                "This is a text@2001-01-03 08:00:00+02"
        };

        TTextSeq firstTemporal = new TTextSeq(value);
        TTextSeq secondTemporal = new TTextSeq(instants);
        TTextSeq thirdTemporal = new TTextSeq(stringInstants);
        TTextSeq fourthTemporal = new TTextSeq(instants, true, false);
        TTextSeq fifthTemporal = new TTextSeq(stringInstants, true, false);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
        assertEquals(firstTemporal, fourthTemporal);
        assertEquals(firstTemporal, fifthTemporal);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-05 08:00:00+02]";
        String secondValue = "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02]";
        String thirdValue = "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]";

        TTextSeq firstTemporal = new TTextSeq(firstValue);
        TTextSeq secondTemporal = new TTextSeq(secondValue);
        TTextSeq thirdTemporal = new TTextSeq(thirdValue);

        assertNotEquals(firstTemporal, secondTemporal);
        assertNotEquals(firstTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, thirdTemporal);
        assertNotEquals(firstTemporal, new Object());
    }

    @Test
    void testSeqType() throws SQLException {
        String value = "(Test1@2001-01-01 08:00:00+02, Test2@2001-01-03 08:00:00+02]";
        TTextSeq temporal = new TTextSeq(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE, temporal.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        String value = String.format(
                "(QWERTY@2001-01-01 08:00:00%1$s, ASDFG@2001-01-03 08:00:00%1$s]",
                format.format(tz)
        );
        TTextSeq temporal = new TTextSeq(value);
        String newValue = temporal.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        List<String> list = tTextSeq.getValues();
        assertEquals(3 , list.size());
        assertEquals("ABCD" , list.get(0));
        assertEquals("LMNO" , list.get(1));
        assertEquals("qwer" , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        assertEquals("ABCD", tTextSeq.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        assertEquals("qwer", tTextSeq.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        assertEquals("ABCD", tTextSeq.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        assertEquals("qwer", tTextSeq.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        assertNull(tTextSeq.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TTextSeq tTextSeq = new TTextSeq(
                "(This is me@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        assertEquals("This is me", tTextSeq.valueAtTimestamp(timestamp));
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        assertEquals(3, tTextSeq.numTimestamps());
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
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        assertEquals(3, tTextSeq.timestamps().length);
        assertEquals(firstExpectedDate, tTextSeq.timestamps()[0]);
        assertEquals(secondExpectedDate, tTextSeq.timestamps()[1]);
        assertEquals(thirdExpectedDate, tTextSeq.timestamps()[2]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        assertEquals(expectedDate, tTextSeq.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tTextSeq.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        assertEquals(expectedDate, tTextSeq.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        assertEquals(expectedDate, tTextSeq.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,false,true);
        assertEquals(period, tTextSeq.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialExpectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endExpectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);

        Period period = new Period(initialExpectedDate,endExpectedDate,false,true);
        PeriodSet periodSet = new PeriodSet(period);
        assertEquals(periodSet, tTextSeq.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        assertEquals(3, tTextSeq.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        TTextInst tTextInst = new TTextInst("ABCD@2001-01-01 08:00:00+02");
        assertEquals(tTextInst, tTextSeq.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        TTextInst tTextInst = new TTextInst("qwer@2001-01-04 08:00:00+02");
        assertEquals(tTextInst, tTextSeq.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        TTextInst tTextInst = new TTextInst("LMNO@2001-01-03 08:00:00+02");
        assertEquals(tTextInst, tTextSeq.instantN(1));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tTextSeq.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        ArrayList<TTextInst> list = new ArrayList<>();
        TTextInst firstTTextInst = new TTextInst("ABCD@2001-01-01 08:00:00+02");
        TTextInst secondTTextInst = new TTextInst("LMNO@2001-01-03 08:00:00+02");
        TTextInst thirdTTextInst = new TTextInst("qwer@2001-01-04 08:00:00+02");
        list.add(firstTTextInst);
        list.add(secondTTextInst);
        list.add(thirdTTextInst);
        assertEquals(3, list.size());
        assertEquals(list, tTextSeq.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tTextSeq.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tTextSeq.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        TTextSeq otherTTextSeq = new TTextSeq(
                "(ABCD@2001-01-03 08:00:00+02, LMNO@2001-01-05 08:00:00+02, qwer@2001-01-06 08:00:00+02]");
        tTextSeq.shift(Duration.ofDays(2));
        assertEquals(otherTTextSeq, tTextSeq);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "[ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, LMNO@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        assertTrue(tTextSeq.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tTextSeq.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tTextSeq.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tTextSeq.intersectsPeriod(period));
    }

    @Test
    void testNumSequences() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        assertEquals(1, tTextSeq.numSequences());
    }

    @Test
    void testStartSequence() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        assertEquals(tTextSeq, tTextSeq.startSequence());
    }

    @Test
    void testEndSequence() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        assertEquals(tTextSeq, tTextSeq.endSequence());
    }

    @Test
    void testSequenceN() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        assertEquals(tTextSeq, tTextSeq.sequenceN(0));
    }

    @Test
    void testSequenceNNoValue() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tTextSeq.sequenceN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no sequence at this index."));
    }

    @Test
    void testSequences() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02]");
        ArrayList<TTextSeq> list = new ArrayList<>();
        list.add(tTextSeq);
        assertEquals(list, tTextSeq.sequences());
    }
}
