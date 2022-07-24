package com.mobilitydb.jdbc.unit.tbool;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tbool.TBoolInst;
import com.mobilitydb.jdbc.tbool.TBoolSeq;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TBoolSeqTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02)";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);
        TBoolInst[] instants = new TBoolInst[]{
                new TBoolInst(true, dateOne),
                new TBoolInst(false, dateTwo)
        };
        String[] stringInstants = new String[]{
                "true@2001-01-01 08:00:00+02",
                "false@2001-01-03 08:00:00+02"
        };

        TBoolSeq firstTemporal = new TBoolSeq(value);
        TBoolSeq secondTemporal = new TBoolSeq(instants);
        TBoolSeq thirdTemporal = new TBoolSeq(stringInstants);
        TBoolSeq fourthTemporal = new TBoolSeq(stringInstants, true, false);
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
        String thirdValue = "[false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)";

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
        String value = "[true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02)";
        TBoolSeq temporal = new TBoolSeq(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE, temporal.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        String value = String.format(
                "[true@2001-01-01 08:00:00%1$s, false@2001-01-03 08:00:00%1$s)",
                tz.toString().substring(0, 3)
        );
        TBoolSeq temporal = new TBoolSeq(value);
        String newValue = temporal.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        List<Boolean> list = tBoolSeq.getValues();
        assertEquals(3 , list.size());
        assertEquals(false , list.get(0));
        assertEquals(true , list.get(1));
        assertEquals(false , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(false, tBoolSeq.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(false, tBoolSeq.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(false, tBoolSeq.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(true, tBoolSeq.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertNull(tBoolSeq.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TBoolSeq tBoolSeq = new TBoolSeq(
                "[false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, false@2001-01-04 08:00:00+02)");
        assertEquals(false, tBoolSeq.valueAtTimestamp(timestamp));
    }
}
