package com.mobilitydb.jdbc.unit.tbool;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tbool.TBoolInst;
import com.mobilitydb.jdbc.tbool.TBoolSeq;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TBoolSeqTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02)";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TBoolInst[] instants = new TBoolInst[]{
                new TBoolInst(true, dateOne),
                new TBoolInst(true, dateTwo)
        };
        String[] stringInstants = new String[]{
                "true@2001-01-01 08:00:00+02",
                "true@2001-01-03 08:00:00+02"
        };

        TBoolSeq firstTemporal = new TBoolSeq(value);
        TBoolSeq secondTemporal = new TBoolSeq(instants);
        TBoolSeq thirdTemporal = new TBoolSeq(stringInstants);
        TBoolSeq fourthTemporal = new TBoolSeq(instants, true, false);
        TBoolSeq fifthTemporal = new TBoolSeq(stringInstants, true, false);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
        assertEquals(firstTemporal, fourthTemporal);
        assertEquals(firstTemporal, fifthTemporal);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02)";
        String secondValue = "(false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02]";
        String thirdValue = "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)";

        TBoolSeq firstTemporal = new TBoolSeq(firstValue);
        TBoolSeq secondTemporal = new TBoolSeq(secondValue);
        TBoolSeq thirdTemporal = new TBoolSeq(thirdValue);

        assertNotEquals(firstTemporal, secondTemporal);
        assertNotEquals(firstTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, thirdTemporal);
        assertNotEquals(firstTemporal, new Object());
    }

    @Test
    void testBoolSeqType() throws SQLException {
        String value = "[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02)";
        TBoolSeq temporal = new TBoolSeq(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE, temporal.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        String value = String.format(
                "(true@2001-01-01 08:00:00%1$s, false@2001-01-03 08:00:00%1$s]",
                format.format(tz)
        );
        TBoolSeq temporal = new TBoolSeq(value);
        String newValue = temporal.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        List<Boolean> list = tBoolSeq.getValues();
        assertEquals(3 , list.size());
        assertEquals(true , list.get(0));
        assertEquals(false , list.get(1));
        assertEquals(false , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(true, tBoolSeq.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(false, tBoolSeq.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(false, tBoolSeq.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(true, tBoolSeq.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertNull(tBoolSeq.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(true, tBoolSeq.valueAtTimestamp(timestamp));
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(3, tBoolSeq.numTimestamps());
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
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(3, tBoolSeq.timestamps().length);
        assertEquals(firstExpectedDate, tBoolSeq.timestamps()[0]);
        assertEquals(secondExpectedDate, tBoolSeq.timestamps()[1]);
        assertEquals(thirdExpectedDate, tBoolSeq.timestamps()[2]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(expectedDate, tBoolSeq.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tBoolSeq.timestampN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(expectedDate, tBoolSeq.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(expectedDate, tBoolSeq.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,true,false);
        assertEquals(period, tBoolSeq.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialExpectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endExpectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);

        Period period = new Period(initialExpectedDate,endExpectedDate,true,false);
        PeriodSet periodSet = new PeriodSet(period);
        assertEquals(periodSet, tBoolSeq.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(3, tBoolSeq.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        TBoolInst tBoolInst = new TBoolInst("true@2001-01-01 08:00:00+02");
        assertEquals(tBoolInst, tBoolSeq.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        TBoolInst tBoolInst = new TBoolInst("false@2001-01-04 08:00:00+02");
        assertEquals(tBoolInst, tBoolSeq.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        TBoolInst tBoolInst = new TBoolInst("false@2001-01-03 08:00:00+02");
        assertEquals(tBoolInst, tBoolSeq.instantN(1));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tBoolSeq.instantN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        ArrayList<TBoolInst> list = new ArrayList<>();
        TBoolInst firstTBoolInst = new TBoolInst("true@2001-01-01 08:00:00+02");
        TBoolInst secondTBoolInst = new TBoolInst("false@2001-01-03 08:00:00+02");
        TBoolInst thirdTBoolInst = new TBoolInst("false@2001-01-04 08:00:00+02");
        list.add(firstTBoolInst);
        list.add(secondTBoolInst);
        list.add(thirdTBoolInst);
        assertEquals(3, list.size());
        assertEquals(list, tBoolSeq.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tBoolSeq.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tBoolSeq.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        TBoolSeq otherTBoolSeq = new TBoolSeq(
                "[true@2001-01-03 08:00:00+02, false@2001-01-05 08:00:00+02, false@2001-01-06 08:00:00+02)");
        tBoolSeq.shift(Duration.ofDays(2));
        assertEquals(otherTBoolSeq, tBoolSeq);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        assertTrue(tBoolSeq.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tBoolSeq.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tBoolSeq.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tBoolSeq.intersectsPeriod(period));
    }

    @Test
    void testNumSequences() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(1, tBoolSeq.numSequences());
    }

    @Test
    void testStartSequence() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(tBoolSeq, tBoolSeq.startSequence());
    }

    @Test
    void testEndSequence() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(tBoolSeq, tBoolSeq.endSequence());
    }

    @Test
    void testSequenceN() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(tBoolSeq, tBoolSeq.sequenceN(0));
    }

    @Test
    void testSequenceNNoValue() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tBoolSeq.sequenceN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no sequence at this index."));
    }

    @Test
    void testSequences() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        ArrayList<TBoolSeq> list = new ArrayList<>();
        list.add(tBoolSeq);
        assertEquals(list, tBoolSeq.sequences());
    }

    @Test
    void testEmpty() {
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> {
                    TBoolInst[] values = new TBoolInst[0];
                    TBoolSeq temporal = new TBoolSeq(values);
                }
        );
        assertTrue(thrown.getMessage().contains(" must be composed of at least one instant."));
    }

    @ParameterizedTest
    @CsvSource({
            "'(false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-01 08:00:00+02]', " +
                    "'The timestamps of a Temporal sequence must be increasing.'",
            "'(false@2001-01-01 08:00:00+02]', " +
                    "'The lower and upper bounds must be inclusive for an instant temporal sequence.'",
            "'[false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02)', " +
                    "'The last two values of a temporal sequence with exclusive upper bound and stepwise interpolation must be equal.'",
    })
    void testInvalids(String value, String error) {
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> {
                    TBoolSeq temporal = new TBoolSeq(value);
                }
        );
        assertTrue(thrown.getMessage().contains(error));
    }
}
