package com.mobilitydb.jdbc.unit.tint;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tint.TIntSeq;
import com.mobilitydb.jdbc.tint.TIntSeqSet;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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
        String value = String.format(
            "{[1@2001-01-01 08:00:00%1$s, 1@2001-01-03 08:00:00%1$s), " +
                "[2@2001-01-04 08:00:00%1$s, 3@2001-01-05 08:00:00%1$s, 3@2001-01-06 08:00:00%1$s]}",
            tz.toString().substring(0, 3)
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
                "{[189@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02), " +
                        "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}");
        assertEquals(189, tIntSeqSet.valueAtTimestamp(timestamp));
    }
}
