package com.mobilitydb.jdbc.unit.tfloat;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tfloat.TFloatInst;
import com.mobilitydb.jdbc.tfloat.TFloatSeq;
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

class TFloatSeqTest {
    @Test
    void testLinearConstructors() throws SQLException {
        String value = "[1.85@2001-01-01 08:00:00+02, 2.85@2001-01-03 08:00:00+02)";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TFloatInst[] instants = new TFloatInst[]{
                new TFloatInst(1.85f, dateOne),
                new TFloatInst(2.85f, dateTwo)
        };
        String[] stringInstants = new String[]{
                "1.85@2001-01-01 08:00:00+02",
                "2.85@2001-01-03 08:00:00+02"
        };

        TFloatSeq firstTemporal = new TFloatSeq(value);
        TFloatSeq secondTemporal = new TFloatSeq(instants);
        TFloatSeq thirdTemporal = new TFloatSeq(stringInstants);
        TFloatSeq fourthTemporal = new TFloatSeq(instants, true, false);
        TFloatSeq fifthTemporal = new TFloatSeq(stringInstants, true, false);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
        assertEquals(firstTemporal, fourthTemporal);
        assertEquals(firstTemporal, fifthTemporal);
    }

    @Test
    void testStepwiseConstructors() throws SQLException {
        String value = "Interp=Stepwise;[2.5@2001-01-01 08:00:00+02, 2.5@2001-01-03 08:00:00+02)";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TFloatInst[] instants = new TFloatInst[]{
                new TFloatInst(2.5f, dateOne),
                new TFloatInst(2.5f, dateTwo)
        };
        String[] stringInstants = new String[]{
                "2.5@2001-01-01 08:00:00+02",
                "2.5@2001-01-03 08:00:00+02"
        };

        TFloatSeq firstTemporal = new TFloatSeq(value);
        TFloatSeq secondTemporal = new TFloatSeq(true,instants);
        TFloatSeq thirdTemporal = new TFloatSeq(true,stringInstants);
        TFloatSeq fourthTemporal = new TFloatSeq(true,instants, true, false);
        TFloatSeq fifthTemporal = new TFloatSeq(true, stringInstants, true, false);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
        assertEquals(firstTemporal, fourthTemporal);
        assertEquals(firstTemporal, fifthTemporal);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "[1.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02)";
        String secondValue = "(1.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02]";
        String thirdValue = "[1.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)";
        String fourthValue = "Interp=Stepwise;(1.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02]";

        TFloatSeq firstTemporal = new TFloatSeq(firstValue);
        TFloatSeq secondTemporal = new TFloatSeq(secondValue);
        TFloatSeq thirdTemporal = new TFloatSeq(thirdValue);
        TFloatSeq fourthTemporal = new TFloatSeq(fourthValue);

        assertNotEquals(firstTemporal, secondTemporal);
        assertNotEquals(firstTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, fourthTemporal);
        assertNotEquals(firstTemporal, new Object());
    }

    @Test
    void testSeqType() throws SQLException {
        String value = "[87.2@2001-01-01 08:00:00+02, 92.1@2001-01-03 08:00:00+02)";
        TFloatSeq temporal = new TFloatSeq(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE, temporal.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        String value = String.format(
                "[7.45@2001-01-01 08:00:00%1$s, 9.81@2001-01-03 08:00:00%1$s)",
                format.format(tz)
        );
        TFloatSeq temporal = new TFloatSeq(value);
        String newValue = temporal.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[1.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        List<Float> list = tFloatSeq.getValues();
        assertEquals(3 , list.size());
        assertEquals(1.23f , list.get(0));
        assertEquals(2.69f , list.get(1));
        assertEquals(3.54f , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[1.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        assertEquals(1.23f, tFloatSeq.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[1.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        assertEquals(3.54f, tFloatSeq.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[8.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        assertEquals(2.69f, tFloatSeq.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[1.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 98.5@2001-01-04 08:00:00+02)");
        assertEquals(98.5f, tFloatSeq.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[1.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        assertNull(tFloatSeq.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        assertEquals(11.23f, tFloatSeq.valueAtTimestamp(timestamp));
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        assertEquals(3, tFloatSeq.numTimestamps());
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
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        assertEquals(3, tFloatSeq.timestamps().length);
        assertEquals(firstExpectedDate, tFloatSeq.timestamps()[0]);
        assertEquals(secondExpectedDate, tFloatSeq.timestamps()[1]);
        assertEquals(thirdExpectedDate, tFloatSeq.timestamps()[2]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        assertEquals(expectedDate, tFloatSeq.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tFloatSeq.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        assertEquals(expectedDate, tFloatSeq.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        assertEquals(expectedDate, tFloatSeq.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,true,false);
        assertEquals(period, tFloatSeq.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialExpectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endExpectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);

        Period period = new Period(initialExpectedDate,endExpectedDate,true,false);
        PeriodSet periodSet = new PeriodSet(period);
        assertEquals(periodSet, tFloatSeq.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        assertEquals(3, tFloatSeq.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        TFloatInst tFloatInst = new TFloatInst("11.23@2001-01-01 08:00:00+02");
        assertEquals(tFloatInst, tFloatSeq.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        TFloatInst tFloatInst = new TFloatInst("3.54@2001-01-04 08:00:00+02");
        assertEquals(tFloatInst, tFloatSeq.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        TFloatInst tFloatInst = new TFloatInst("2.69@2001-01-03 08:00:00+02");
        assertEquals(tFloatInst, tFloatSeq.instantN(1));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> {
                    tFloatSeq.instantN(4);
                }
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        ArrayList<TFloatInst> list = new ArrayList<>();
        TFloatInst firstTFloatInst = new TFloatInst("11.23@2001-01-01 08:00:00+02");
        TFloatInst secondTFloatInst = new TFloatInst("2.69@2001-01-03 08:00:00+02");
        TFloatInst thirdTFloatInst= new TFloatInst("3.54@2001-01-04 08:00:00+02");
        list.add(firstTFloatInst);
        list.add(secondTFloatInst);
        list.add(thirdTFloatInst);
        assertEquals(3, list.size());
        assertEquals(list, tFloatSeq.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tFloatSeq.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tFloatSeq.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        TFloatSeq otherTFloatSeq = new TFloatSeq(
                "[11.23@2001-01-03 08:00:00+02, 2.69@2001-01-05 08:00:00+02, 3.54@2001-01-06 08:00:00+02)");
        tFloatSeq.shift(Duration.ofDays(2));
        assertEquals(otherTFloatSeq, tFloatSeq);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        assertTrue(tFloatSeq.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tFloatSeq.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tFloatSeq.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tFloatSeq.intersectsPeriod(period));
    }

    @Test
    void testNumSequences() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        assertEquals(1, tFloatSeq.numSequences());
    }

    @Test
    void testStartSequence() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        assertEquals(tFloatSeq, tFloatSeq.startSequence());
    }

    @Test
    void testEndSequence() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        assertEquals(tFloatSeq, tFloatSeq.endSequence());
    }

    @Test
    void testSequenceN() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        assertEquals(tFloatSeq, tFloatSeq.sequenceN(0));
    }

    @Test
    void testSequenceNNoValue() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> {
                    tFloatSeq.sequenceN(4);
                }
        );
        assertTrue(thrown.getMessage().contains("There is no sequence at this index."));
    }

    @Test
    void testSequences() throws SQLException {
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[11.23@2001-01-01 08:00:00+02, 2.69@2001-01-03 08:00:00+02, 3.54@2001-01-04 08:00:00+02)");
        ArrayList<TFloatSeq> list = new ArrayList<>();
        list.add(tFloatSeq);
        assertEquals(list, tFloatSeq.sequences());
    }
}
