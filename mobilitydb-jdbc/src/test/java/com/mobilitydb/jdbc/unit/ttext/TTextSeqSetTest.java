package com.mobilitydb.jdbc.unit.ttext;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.ttext.TTextSeq;
import com.mobilitydb.jdbc.ttext.TTextSeqSet;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TTextSeqSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{[A@2001-01-01 08:00:00+02, B@2001-01-03 08:00:00+02), " +
                "[C@2001-01-04 08:00:00+02, D@2001-01-05 08:00:00+02, E@2001-01-06 08:00:00+02]}";

        TTextSeq[] sequences = new TTextSeq[]{
                new TTextSeq("[A@2001-01-01 08:00:00+02, B@2001-01-03 08:00:00+02)"),
                new TTextSeq("[C@2001-01-04 08:00:00+02, D@2001-01-05 08:00:00+02, E@2001-01-06 08:00:00+02]")
        };
        String[] stringSequences = new String[]{
                "[A@2001-01-01 08:00:00+02, B@2001-01-03 08:00:00+02)",
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
        String firstValue = "{[aaa@2001-01-01 08:00:00+02, bbb@2001-01-03 08:00:00+02), " +
                "[ccc@2001-01-04 08:00:00+02, ddd@2001-01-05 08:00:00+02, eee@2001-01-06 08:00:00+02]}";
        String secondValue = "{[aaa@2001-01-01 08:00:00+02, bbb@2001-01-03 08:00:00+02)}";
        String thirdValue = "{[aaa@2001-01-01 08:00:00+02, bbb@2001-01-03 08:00:00+02), " +
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
        String value = "{[abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02), " +
                "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}";
        TTextSeqSet temporal = new TTextSeqSet(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE_SET, temporal.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        String value = String.format(
                "{[abc@2001-01-01 08:00:00%1$s, def@2001-01-03 08:00:00%1$s), " +
                        "[ghi@2001-01-04 08:00:00%1$s, jkl@2001-01-05 08:00:00%1$s, mno@2001-01-06 08:00:00%1$s]}",
                tz.toString().substring(0, 3)
        );
        TTextSeqSet temporal = new TTextSeqSet(value);
        String newValue = temporal.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{[abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02), " +
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
                "{[abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02), " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertEquals("abc", tTextSeqSet.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{[abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02), " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertEquals("mno", tTextSeqSet.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{[abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02), " +
                        "[ghi@2001-01-04 08:00:00+02, aaa@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertEquals("aaa", tTextSeqSet.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{[abc@2001-01-01 08:00:00+02, zzz@2001-01-03 08:00:00+02), " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertEquals("zzz", tTextSeqSet.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{[abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02), " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertNull(tTextSeqSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 4,
                8, 0, 0, 0, tz);
        TTextSeqSet tTextSeqSet = new TTextSeqSet(
                "{[abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02), " +
                        "[ghi@2001-01-04 08:00:00+02, jkl@2001-01-05 08:00:00+02, mno@2001-01-06 08:00:00+02]}");
        assertEquals("ghi", tTextSeqSet.valueAtTimestamp(timestamp));
    }
}
