package com.mobilitydb.jdbc.unit.ttext;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.ttext.TTextInst;
import com.mobilitydb.jdbc.ttext.TTextInstSet;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TTextInstSetTest {
    @Test
    void testConstructors() throws SQLException {
        String value = "{Random@2001-01-01 08:00:00+02, Random2@2001-01-03 08:00:00+02}";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime dateOne = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        OffsetDateTime dateTwo = OffsetDateTime.of(2001,1, 3,
                8, 0, 0, 0, tz);

        TTextInstSet tTextInstSet = new TTextInstSet(value);
        TTextInst[] instants = new TTextInst[]{
                new TTextInst("Random", dateOne),
                new TTextInst("Random2", dateTwo)
        };
        TTextInstSet otherTIntInstSet = new TTextInstSet(instants);

        assertEquals(tTextInstSet.getValues(), otherTIntInstSet.getValues());
        assertEquals(tTextInstSet, otherTIntInstSet);
    }

    @Test
    void testNotEquals() throws SQLException {
        String firstValue = "{abc@2001-01-01 08:00:00+02, def@2001-01-03 08:00:00+02}";
        String secondValue = "{jkl@2001-01-01 08:00:00+02, mno@2001-01-03 08:00:00+02}";
        String thirdValue = "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}";

        TTextInstSet firstTIntInstSet = new TTextInstSet(firstValue);
        TTextInstSet secondTIntInstSet = new TTextInstSet(secondValue);
        TTextInstSet thirdTIntInstSet = new TTextInstSet(thirdValue);

        assertNotEquals(firstTIntInstSet, secondTIntInstSet);
        assertNotEquals(firstTIntInstSet, thirdTIntInstSet);
        assertNotEquals(secondTIntInstSet, thirdTIntInstSet);
        assertNotEquals(firstTIntInstSet, new Object());
    }

    @Test
    void testStringArrayStringConstructors() throws SQLException {
        String value = "{ABCD@2001-01-01 08:00:00+02, JKLM@2001-01-03 08:00:00+02}";

        TTextInstSet tTextInstSet = new TTextInstSet(value);
        String[] instants = new String[]{
                "ABCD@2001-01-01 08:00:00+02",
                "JKLM@2001-01-03 08:00:00+02"
        };
        TTextInstSet otherTTextInstSet = new TTextInstSet(instants);

        assertEquals(tTextInstSet.getValues(), otherTTextInstSet.getValues());
    }

    @Test
    void testInstSetType() throws SQLException {
        String value = "{Test@2001-01-01 08:00:00+02, Other@2001-01-03 08:00:00+02}";
        TTextInstSet tTextInstSet = new TTextInstSet(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tTextInstSet.getTemporalType());
    }

    @Test
    void testBuildValue() throws SQLException {
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        String value = String.format(
                "{this@2001-01-01 08:00:00%1$s, that@2001-01-03 08:00:00%1$s}",
                tz.toString().substring(0, 3)
        );
        TTextInstSet tIntItTextInstSetstSet = new TTextInstSet(value);
        String newValue = tIntItTextInstSetstSet.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        List<String> list = tTextInstSet.getValues();
        assertEquals(3 , list.size());
        assertEquals("pqr" , list.get(0));
        assertEquals("stu" , list.get(1));
        assertEquals("vwx" , list.get(2));
    }

    @Test
    void testStartValue() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        assertEquals("pqr", tTextInstSet.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        assertEquals("vwx", tTextInstSet.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{abc@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        assertEquals("abc", tTextInstSet.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, jkl@2001-01-04 08:00:00+02}");
        assertEquals("stu", tTextInstSet.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,9, 8,
                6, 4, 32, 0, tz);
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        assertNull(tTextInstSet.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2001,1, 1,
                8, 0, 0, 0, tz);
        TTextInstSet tTextInstSet = new TTextInstSet(
                "{pqr@2001-01-01 08:00:00+02, stu@2001-01-03 08:00:00+02, vwx@2001-01-04 08:00:00+02}");
        assertEquals("pqr", tTextInstSet.valueAtTimestamp(timestamp));
    }
}
