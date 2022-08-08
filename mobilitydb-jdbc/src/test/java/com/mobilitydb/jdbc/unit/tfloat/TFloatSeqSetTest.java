package com.mobilitydb.jdbc.unit.tfloat;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tfloat.TFloatInst;
import com.mobilitydb.jdbc.tfloat.TFloatSeq;
import com.mobilitydb.jdbc.tfloat.TFloatSeqSet;
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

class TFloatSeqSetTest {
    @Test
    void testLinearConstructors() throws SQLException {
        String value = "{[1.8@2001-01-01 08:00:00+02, 1.9@2001-01-03 08:00:00+02), " +
                "[2.3@2001-01-04 08:00:00+02, 3.4@2001-01-05 08:00:00+02, 3.5@2001-01-06 08:00:00+02]}";

        TFloatSeq[] sequences = new TFloatSeq[]{
                new TFloatSeq("[1.8@2001-01-01 08:00:00+02, 1.9@2001-01-03 08:00:00+02)"),
                new TFloatSeq("[2.3@2001-01-04 08:00:00+02, 3.4@2001-01-05 08:00:00+02, 3.5@2001-01-06 08:00:00+02]")
        };
        String[] stringSequences = new String[]{
                "[1.8@2001-01-01 08:00:00+02, 1.9@2001-01-03 08:00:00+02)",
                "[2.3@2001-01-04 08:00:00+02, 3.4@2001-01-05 08:00:00+02, 3.5@2001-01-06 08:00:00+02]"
        };

        TFloatSeqSet firstTemporal = new TFloatSeqSet(value);
        TFloatSeqSet secondTemporal = new TFloatSeqSet(sequences);
        TFloatSeqSet thirdTemporal = new TFloatSeqSet(stringSequences);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
    }

    @Test
    void testStepwiseConstructors() throws SQLException {
        String value = "Interp=Stepwise;{(1.8@2001-01-01 08:00:00+02, 1.9@2001-01-03 08:00:00+02], " +
                "[2.3@2001-01-04 08:00:00+02, 3.4@2001-01-05 08:00:00+02, 3.5@2001-01-06 08:00:00+02]}";

        TFloatSeq[] sequences = new TFloatSeq[]{
                new TFloatSeq("Interp=Stepwise;(1.8@2001-01-01 08:00:00+02, 1.9@2001-01-03 08:00:00+02]"),
                new TFloatSeq("Interp=Stepwise;[2.3@2001-01-04 08:00:00+02, 3.4@2001-01-05 08:00:00+02, " +
                        "3.5@2001-01-06 08:00:00+02]")
        };
        String[] stringSequences = new String[]{
                "Interp=Stepwise;(1.8@2001-01-01 08:00:00+02, 1.9@2001-01-03 08:00:00+02]",
                "Interp=Stepwise;[2.3@2001-01-04 08:00:00+02, 3.4@2001-01-05 08:00:00+02, 3.5@2001-01-06 08:00:00+02]"
        };

        TFloatSeqSet firstTemporal = new TFloatSeqSet(value);
        TFloatSeqSet secondTemporal = new TFloatSeqSet(true, sequences);
        TFloatSeqSet thirdTemporal = new TFloatSeqSet(true,stringSequences);

        assertEquals(firstTemporal.getValues(), secondTemporal.getValues());
        assertEquals(firstTemporal, secondTemporal);
        assertEquals(firstTemporal, thirdTemporal);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{[1.2@2001-01-01 08:00:00+02, 1.3@2001-01-03 08:00:00+02), " +
                "[2.1@2001-01-04 08:00:00+02, 3.8@2001-01-05 08:00:00+02, 3.9@2001-01-06 08:00:00+02]}";
        String secondValue = "{[1.2@2001-01-01 08:00:00+02, 1.3@2001-01-03 08:00:00+02)}";
        String thirdValue = "{[1.2@2001-01-01 08:00:00+02, 1.3@2001-01-03 08:00:00+02), " +
                "[2.1@2001-01-04 08:00:00+02, 3.8@2001-01-05 08:00:00+02]}";

        TFloatSeqSet firstTemporal = new TFloatSeqSet(firstValue);
        TFloatSeqSet secondTemporal = new TFloatSeqSet(secondValue);
        TFloatSeqSet thirdTemporal = new TFloatSeqSet(thirdValue);

        assertNotEquals(firstTemporal, secondTemporal);
        assertNotEquals(firstTemporal, thirdTemporal);
        assertNotEquals(secondTemporal, thirdTemporal);
        assertNotEquals(firstTemporal, new Object());
    }

    @Test
    void testSeqSetType() throws SQLException {
        String value = "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}";
        TFloatSeqSet temporal = new TFloatSeqSet(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE_SET, temporal.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        DateTimeFormatter format = DateTimeFormatter.ofPattern("X");
        String value = String.format(
                "{[1.2@2001-01-01 08:00:00%1$s, 1.5@2001-01-03 08:00:00%1$s), " +
                        "[2.8@2001-01-04 08:00:00%1$s, 3.2@2001-01-05 08:00:00%1$s, 3.3@2001-01-06 08:00:00%1$s]}",
                format.format(tz)
        );
        TFloatSeqSet temporal = new TFloatSeqSet(value);
        String newValue = temporal.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        List<Float> list = tFloatSeqSet.getValues();
        assertEquals(5 , list.size());
        assertEquals(12.5f , list.get(0));
        assertEquals(13.7f , list.get(1));
        assertEquals(25.6f , list.get(2));
        assertEquals(37.9f , list.get(3));
        assertEquals(39.6f , list.get(4));
    }

    @Test
    void testStartValue() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        assertEquals(12.5f, tFloatSeqSet.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        assertEquals(39.6f, tFloatSeqSet.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        assertEquals(12.5f, tFloatSeqSet.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 87.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        assertEquals(87.9f, tFloatSeqSet.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        assertNull(tFloatSeqSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        assertEquals(12.5f, tFloatSeqSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testNumTimestamps() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        assertEquals(5, tFloatSeqSet.numTimestamps());
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
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        assertEquals(5, tFloatSeqSet.timestamps().length);
        assertEquals(firstExpectedDate, tFloatSeqSet.timestamps()[0]);
        assertEquals(secondExpectedDate, tFloatSeqSet.timestamps()[1]);
        assertEquals(thirdExpectedDate, tFloatSeqSet.timestamps()[2]);
        assertEquals(fourthExpectedDate, tFloatSeqSet.timestamps()[3]);
        assertEquals(fifthExpectedDate, tFloatSeqSet.timestamps()[4]);
    }

    @Test
    void testTimestampN() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        assertEquals(expectedDate, tFloatSeqSet.timestampN(1));
    }

    @Test
    void testTimestampNNoValue() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tFloatSeqSet.timestampN(6)
        );
        assertTrue(thrown.getMessage().contains("There is no timestamp at this index."));
    }

    @Test
    void testStartTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        assertEquals(expectedDate, tFloatSeqSet.startTimestamp());
    }

    @Test
    void testEndTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        assertEquals(expectedDate, tFloatSeqSet.endTimestamp());
    }

    @Test
    void testPeriod() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        Period period = new Period(initialDate, endDate,true,true);
        assertEquals(period, tFloatSeqSet.period());
    }

    @Test
    void testGetTime() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialExpectedDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endExpectedDate = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        Period firstPeriod = new Period(initialExpectedDate,endExpectedDate,true,false);
        initialExpectedDate = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        endExpectedDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        Period secondPeriod = new Period(initialExpectedDate,endExpectedDate,true,true);
        PeriodSet periodSet = new PeriodSet(firstPeriod, secondPeriod);
        assertEquals(periodSet, tFloatSeqSet.getTime());
    }

    @Test
    void testNumInstants() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        assertEquals(5, tFloatSeqSet.numInstants());
    }

    @Test
    void testStartInstant() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        TFloatInst tFloatInst = new TFloatInst("12.5@2001-01-01 08:00:00+02");
        assertEquals(tFloatInst, tFloatSeqSet.startInstant());
    }

    @Test
    void testEndInstant() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        TFloatInst tFloatInst = new TFloatInst("39.6@2001-01-06 08:00:00+02");
        assertEquals(tFloatInst, tFloatSeqSet.endInstant());
    }

    @Test
    void testInstantN() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        TFloatInst tFloatInst = new TFloatInst("37.9@2001-01-05 08:00:00+02");
        assertEquals(tFloatInst, tFloatSeqSet.instantN(3));
    }

    @Test
    void testInstantNNoValue() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tFloatSeqSet.instantN(6)
        );
        assertTrue(thrown.getMessage().contains("There is no instant at this index."));
    }

    @Test
    void testGetInstants() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        ArrayList<TFloatInst> list = new ArrayList<>();
        TFloatInst firstInst = new TFloatInst("12.5@2001-01-01 08:00:00+02");
        TFloatInst secondInst = new TFloatInst("13.7@2001-01-03 08:00:00+02");
        TFloatInst thirdTInst = new TFloatInst("25.6@2001-01-04 08:00:00+02");
        TFloatInst fourthInst = new TFloatInst("37.9@2001-01-05 08:00:00+02");
        TFloatInst fifthInst = new TFloatInst("39.6@2001-01-06 08:00:00+02");
        list.add(firstInst);
        list.add(secondInst);
        list.add(thirdTInst);
        list.add(fourthInst);
        list.add(fifthInst);
        assertEquals(5, list.size());
        assertEquals(list, tFloatSeqSet.instants());
    }

    @Test
    void testDuration() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
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
        assertEquals(expectedDuration, tFloatSeqSet.duration());
    }

    @Test
    void testTimespan() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime initialDate = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime endDate = OffsetDateTime.of(2001,1, 6,
                8, 0, 0, 0, tz);
        Duration expectedDuration = Duration.between(initialDate, endDate);
        assertEquals(expectedDuration, tFloatSeqSet.timespan());
    }

    @Test
    void testShift() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        TFloatSeqSet otherTFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-03 08:00:00+02, 13.7@2001-01-05 08:00:00+02), " +
                        "[25.6@2001-01-06 08:00:00+02, 37.9@2001-01-07 08:00:00+02, 39.6@2001-01-08 08:00:00+02]}");
        tFloatSeqSet.shift(Duration.ofDays(2));
        assertEquals(otherTFloatSeqSet, tFloatSeqSet);
    }

    @Test
    void testIntersectsTimestamp() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        assertTrue(tFloatSeqSet.intersectsTimestamp(date));
    }

    @Test
    void testNoIntersectsTimestamp() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime date = OffsetDateTime.of(2021,1, 1,
                8, 0, 0, 0, tz);
        assertFalse(tFloatSeqSet.intersectsTimestamp(date));
    }

    @Test
    void testIntersectsPeriod() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        Period period = new Period("[2001-01-02 08:00:00+02, 2001-01-10 00:00:00+01)");
        assertTrue(tFloatSeqSet.intersectsPeriod(period));
    }

    @Test
    void testNoIntersectsPeriod() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        Period period = new Period("[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)");
        assertFalse(tFloatSeqSet.intersectsPeriod(period));
    }

    @Test
    void testNumSequences() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        assertEquals(2, tFloatSeqSet.numSequences());
    }

    @Test
    void testStartSequence() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        TFloatSeq tFloatSeq = new TFloatSeq("[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02)");
        assertEquals(tFloatSeq, tFloatSeqSet.startSequence());
    }

    @Test
    void testEndSequence() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]");
        assertEquals(tFloatSeq, tFloatSeqSet.endSequence());
    }

    @Test
    void testSequenceN() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        TFloatSeq tFloatSeq = new TFloatSeq(
                "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]");
        assertEquals(tFloatSeq, tFloatSeqSet.sequenceN(1));
    }

    @Test
    void testSequenceNNoValue() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tFloatSeqSet.sequenceN(4)
        );
        assertTrue(thrown.getMessage().contains("There is no sequence at this index."));
    }

    @Test
    void testSequences() throws SQLException {
        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(
                "{[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02), " +
                        "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]}");
        ArrayList<TFloatSeq> list = new ArrayList<>();
        list.add(new TFloatSeq(
                "[12.5@2001-01-01 08:00:00+02, 13.7@2001-01-03 08:00:00+02)"));
        list.add(new TFloatSeq(
                "[25.6@2001-01-04 08:00:00+02, 37.9@2001-01-05 08:00:00+02, 39.6@2001-01-06 08:00:00+02]"));
        assertEquals(list, tFloatSeqSet.sequences());
    }
}
