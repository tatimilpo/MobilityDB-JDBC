package com.mobilitydb.jdbc.unit.tfloat;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tfloat.TFloatInst;
import com.mobilitydb.jdbc.tfloat.TFloatInstSet;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TFloatInstSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{1.8@2001-01-01 08:00:00+02, 2.8@2001-01-03 08:00:00+02}";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);

        TFloatInstSet tFloatInstSet = new TFloatInstSet(value);
        TFloatInst[] instants = new TFloatInst[]{
                new TFloatInst(1.8f, dateOne),
                new TFloatInst(2.8f, dateTwo)
        };
        TFloatInstSet otherTIntInstSet = new TFloatInstSet(instants);

        assertEquals(tFloatInstSet.getValues(), otherTIntInstSet.getValues());
        assertEquals(tFloatInstSet, otherTIntInstSet);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{1.45@2001-01-01 08:00:00+02, 2.2@2001-01-03 08:00:00+02}";
        String secondValue = "{2.69@2001-01-01 08:00:00+02, 3.85@2001-01-03 08:00:00+02}";
        String thirdValue = "{1.77@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}";

        TFloatInstSet firstTFloatInstSet = new TFloatInstSet(firstValue);
        TFloatInstSet secondTFloatInstSet = new TFloatInstSet(secondValue);
        TFloatInstSet thirdTFloatInstSet = new TFloatInstSet(thirdValue);

        assertNotEquals(firstTFloatInstSet, secondTFloatInstSet);
        assertNotEquals(firstTFloatInstSet, thirdTFloatInstSet);
        assertNotEquals(secondTFloatInstSet, thirdTFloatInstSet);
        assertNotEquals(firstTFloatInstSet, new Object());
    }

    @Test
    void testStringArrayStringConstructors() throws SQLException {
        String value = "{19.8@2001-01-01 08:00:00+02, 2.52@2001-01-03 08:00:00+02}";

        TFloatInstSet tFloatInstSet = new TFloatInstSet(value);
        String[] instants = new String[]{
                "19.8@2001-01-01 08:00:00+02",
                "2.52@2001-01-03 08:00:00+02"
        };
        TFloatInstSet otherTFloatInstSet = new TFloatInstSet(instants);

        assertEquals(tFloatInstSet.getValues(), otherTFloatInstSet.getValues());
    }

    @Test
    void testInstSetType() throws SQLException {
        String value = "{1.11@2001-01-01 08:00:00+02, 2.22@2001-01-03 08:00:00+02}";
        TFloatInstSet tFloatInstSet = new TFloatInstSet(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tFloatInstSet.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        String value = String.format(
                "{47.5@2001-01-01 08:00:00%1$s, 96.5@2001-01-03 08:00:00%1$s}",
                tz.toString().substring(0, 3)
        );
        TFloatInstSet tFloatInstSet = new TFloatInstSet(value);
        String newValue = tFloatInstSet.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.77@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        List<Float> list = tFloatInstSet.getValues();
        assertEquals(3 , list.size());
        assertEquals(1.77f , list.get(0));
        assertEquals(2.1f , list.get(1));
        assertEquals(3.78f , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.77@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertEquals(1.77f, tFloatInstSet.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.77@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertEquals(3.78f, tFloatInstSet.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.77@2001-01-01 08:00:00+02, 0.5@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertEquals(0.5f, tFloatInstSet.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{8.9@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertEquals(8.9f, tFloatInstSet.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.77@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertNull(tFloatInstSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TFloatInstSet tFloatInstSet = new TFloatInstSet(
                "{1.82@2001-01-01 08:00:00+02, 2.1@2001-01-03 08:00:00+02, 3.78@2001-01-04 08:00:00+02}");
        assertEquals(1.82f, tFloatInstSet.valueAtTimestamp(timestamp));
    }
}
