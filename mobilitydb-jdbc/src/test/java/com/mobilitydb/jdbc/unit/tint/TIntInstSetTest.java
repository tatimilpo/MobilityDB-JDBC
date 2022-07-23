package com.mobilitydb.jdbc.unit.tint;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tint.TIntInst;
import com.mobilitydb.jdbc.tint.TIntInstSet;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TIntInstSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02}";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);

        TIntInstSet tIntInstSet = new TIntInstSet(value);
        TIntInst[] instants = new TIntInst[]{
            new TIntInst(1, dateOne),
            new TIntInst(2, dateTwo)
        };
        TIntInstSet otherTIntInstSet = new TIntInstSet(instants);

        assertEquals(tIntInstSet.getValues(), otherTIntInstSet.getValues());
        assertEquals(tIntInstSet, otherTIntInstSet);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02}";
        String secondValue = "{2@2001-01-01 08:00:00+02, 3@2001-01-03 08:00:00+02}";
        String thirdValue = "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}";

        TIntInstSet firstTIntInstSet = new TIntInstSet(firstValue);
        TIntInstSet secondTIntInstSet = new TIntInstSet(secondValue);
        TIntInstSet thirdTIntInstSet = new TIntInstSet(thirdValue);

        assertNotEquals(firstTIntInstSet, secondTIntInstSet);
        assertNotEquals(firstTIntInstSet, thirdTIntInstSet);
        assertNotEquals(secondTIntInstSet, thirdTIntInstSet);
        assertNotEquals(firstTIntInstSet, new Object());
    }

    @Test
    void testStringArrayStringConstructors() throws SQLException {
        String value = "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02}";

        TIntInstSet tIntInstSet = new TIntInstSet(value);
        String[] instants = new String[]{
            "1@2001-01-01 08:00:00+02",
            "2@2001-01-03 08:00:00+02"
        };
        TIntInstSet otherTIntInstSet = new TIntInstSet(instants);

        assertEquals(tIntInstSet.getValues(), otherTIntInstSet.getValues());
    }

    @Test
    void testInstSetType() throws SQLException {
        String value = "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02}";
        TIntInstSet tIntInstSet = new TIntInstSet(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tIntInstSet.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        String value = String.format(
            "{1@2001-01-01 08:00:00%1$s, 2@2001-01-03 08:00:00%1$s}",
            tz.toString().substring(0, 3)
        );
        TIntInstSet tIntInstSet = new TIntInstSet(value);
        String newValue = tIntInstSet.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        List<Integer> list = tIntInstSet.getValues();
        assertEquals(3 , list.size());
        assertEquals(1 , list.get(0));
        assertEquals(2 , list.get(1));
        assertEquals(3 , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{8@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        assertEquals(8, tIntInstSet.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        assertEquals(3, tIntInstSet.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{15@2001-01-01 08:00:00+02, 87@2001-01-03 08:00:00+02, 43@2001-01-04 08:00:00+02}");
        assertEquals(15, tIntInstSet.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{178@2001-01-01 08:00:00+02, 252@2001-01-03 08:00:00+02, 43@2001-01-04 08:00:00+02}");
        assertEquals(252, tIntInstSet.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        assertNull(tIntInstSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TIntInstSet tIntInstSet = new TIntInstSet(
                "{18@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02, 3@2001-01-04 08:00:00+02}");
        assertEquals(18, tIntInstSet.valueAtTimestamp(timestamp));
    }
}
