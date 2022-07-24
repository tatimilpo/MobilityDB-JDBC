package com.mobilitydb.jdbc.unit.ttext;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.ttext.TTextInst;
import com.mobilitydb.jdbc.ttext.TTextSeq;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TTextSeqTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "[This is a text@2001-01-01 08:00:00+02, This is a text too@2001-01-03 08:00:00+02)";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TTextInst[] instants = new TTextInst[]{
                new TTextInst("This is a text", dateOne),
                new TTextInst("This is a text too", dateTwo)
        };
        String[] stringInstants = new String[]{
                "This is a text@2001-01-01 08:00:00+02",
                "This is a text too@2001-01-03 08:00:00+02"
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
        String firstValue = "[ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02)";
        String secondValue = "(ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02]";
        String thirdValue = "[ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02)";

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
        String value = "[Test1@2001-01-01 08:00:00+02, Test2@2001-01-03 08:00:00+02)";
        TTextSeq temporal = new TTextSeq(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE, temporal.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        String value = String.format(
                "[QWERTY@2001-01-01 08:00:00%1$s, ASDFG@2001-01-03 08:00:00%1$s)",
                tz.toString().substring(0, 3)
        );
        TTextSeq temporal = new TTextSeq(value);
        String newValue = temporal.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "[ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02)");
        List<String> list = tTextSeq.getValues();
        assertEquals(3 , list.size());
        assertEquals("ABCD" , list.get(0));
        assertEquals("LMNO" , list.get(1));
        assertEquals("qwer" , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "[ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02)");
        assertEquals("ABCD", tTextSeq.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "[ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02)");
        assertEquals("qwer", tTextSeq.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "[ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02)");
        assertEquals("ABCD", tTextSeq.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TTextSeq tTextSeq = new TTextSeq(
                "[ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02)");
        assertEquals("qwer", tTextSeq.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TTextSeq tTextSeq = new TTextSeq(
                "[ABCD@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02)");
        assertNull(tTextSeq.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TTextSeq tTextSeq = new TTextSeq(
                "[This is me@2001-01-01 08:00:00+02, LMNO@2001-01-03 08:00:00+02, qwer@2001-01-04 08:00:00+02)");
        assertEquals("This is me", tTextSeq.valueAtTimestamp(timestamp));
    }
}
