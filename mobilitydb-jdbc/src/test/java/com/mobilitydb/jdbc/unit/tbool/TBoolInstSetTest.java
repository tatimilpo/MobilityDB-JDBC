package com.mobilitydb.jdbc.unit.tbool;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tbool.TBoolInst;
import com.mobilitydb.jdbc.tbool.TBoolInstSet;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TBoolInstSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02}";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);

        TBoolInstSet tBoolInstSet = new TBoolInstSet(value);
        TBoolInst[] instants = new TBoolInst[]{
            new TBoolInst(false, dateOne),
            new TBoolInst(true, dateTwo)
        };
        TBoolInstSet otherTBoolInstSet = new TBoolInstSet(instants);

        assertEquals(tBoolInstSet.getValues(), otherTBoolInstSet.getValues());
        assertEquals(tBoolInstSet, otherTBoolInstSet);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02}";
        String secondValue = "{true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02}";
        String thirdValue = "{false@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}";

        TBoolInstSet firstTBoolInstSet = new TBoolInstSet(firstValue);
        TBoolInstSet secondTBoolInstSet = new TBoolInstSet(secondValue);
        TBoolInstSet thirdTBoolInstSet = new TBoolInstSet(thirdValue);

        assertNotEquals(firstTBoolInstSet, secondTBoolInstSet);
        assertNotEquals(firstTBoolInstSet, thirdTBoolInstSet);
        assertNotEquals(secondTBoolInstSet, thirdTBoolInstSet);
        assertNotEquals(firstTBoolInstSet, new Object());
    }

    @Test
    void testStringArrayStringConstructors() throws SQLException {
        String value = "{true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02}";

        TBoolInstSet tBoolInstSet = new TBoolInstSet(value);
        String[] instants = new String[]{
            "true@2001-01-01 08:00:00+02",
            "false@2001-01-03 08:00:00+02"
        };
        TBoolInstSet otherTBoolInstSet = new TBoolInstSet(instants);

        assertEquals(tBoolInstSet.getValues(), otherTBoolInstSet.getValues());
    }

    @Test
    void testBoolSetType() throws SQLException {
        String value = "{{true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02}";
        TBoolInstSet tBoolInstSet = new TBoolInstSet(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tBoolInstSet.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        String value = String.format(
            "{true@2001-01-01 08:00:00%1$s, false@2001-01-03 08:00:00%1$s}",
            tz.toString().substring(0, 3)
        );
        TBoolInstSet tBoolInstSet = new TBoolInstSet(value);
        String newValue = tBoolInstSet.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        List<Boolean> list = tBoolInstSet.getValues();
        assertEquals(3 , list.size());
        assertEquals(false , list.get(0));
        assertEquals(true , list.get(1));
        assertEquals(true , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertEquals(false, tBoolInstSet.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertEquals(true, tBoolInstSet.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertEquals(false, tBoolInstSet.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertEquals(true, tBoolInstSet.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertNull(tBoolInstSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TBoolInstSet tBoolInstSet = new TBoolInstSet(
                "{false@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02, true@2001-01-04 08:00:00+02}");
        assertEquals(false, tBoolInstSet.valueAtTimestamp(timestamp));
    }
}
