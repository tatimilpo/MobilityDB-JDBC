package com.mobilitydb.jdbc.unit.ttext;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.ttext.TTextInst;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TTextInstTest {
    @Test
    void testConstructor() throws SQLException {
        String value = "abccccd@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);

        TTextInst tTextInst = new TTextInst(value);
        TTextInst other = new TTextInst("abccccd", otherDate);

        assertEquals(other.getTemporalValue(), tTextInst.getTemporalValue());
        assertEquals(other, tTextInst);
    }

    @Test
    void testEquals() throws SQLException {
        String value = "abcd@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        String otherValue = "ijfi@2019-09-09 06:04:32+02";

        TTextInst first = new TTextInst(value);
        TTextInst second = new TTextInst("abcd", otherDate);
        TTextInst third = new TTextInst(otherValue);

        assertEquals(first, second);
        assertNotEquals(first, third);
        assertNotEquals(second, third);
        assertNotEquals(first, new Object());
    }


    @ParameterizedTest
    @ValueSource(strings = {
            "abcd@2019-09-08 06:04:32+02",
            "TEST@2019-09-08 06:04:32+02",
            "This is a Test@2019-09-08 06:04:32+02"
    })
    void testInstSetType(String value) throws SQLException {
        TTextInst tTextInst = new TTextInst(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT, tTextInst.getTemporalType());
    }

    @Test
    void testInvalidEmptyValue() {
        SQLException thrown = assertThrows(
            SQLException.class,
            () -> {
                TTextInst tTextInst = new TTextInst("");
            }
        );
        assertTrue(thrown.getMessage().contains("Value cannot be empty."));
    }

    @Test
    void testBuildValue() throws SQLException {
        String value = "sfcsff@2019-09-08 06:04:32";
        ZoneOffset tz = OffsetDateTime.now().getOffset();
        value = value + tz.toString().substring(0, 3);
        TTextInst tIntInst = new TTextInst(value);
        String newValue = tIntInst.buildValue();
        assertEquals(value, newValue);
    }

    @Test
    void testGetValues() throws SQLException {
        String value = "Test@2019-09-08 06:04:32+02";
        TTextInst tTextInst = new TTextInst(value);
        List<String> values = tTextInst.getValues();
        assertEquals(1, values.size());
        assertEquals("Test", values.get(0));
    }

    @Test
    void testGetValue() throws SQLException {
        TTextInst tTextInst = new TTextInst("test@2019-09-08 06:04:32+02");
        assertEquals("test", tTextInst.getValue());
    }

    @Test
    void testStartValue() throws SQLException {
        TTextInst tTextInst = new TTextInst("test12@2019-09-08 06:04:32+02");
        assertEquals("test12", tTextInst.startValue());
    }

    @Test
    void testEndValue() throws SQLException {
        TTextInst tTextInst = new TTextInst("test145@2019-09-08 06:04:32+02");
        assertEquals("test145", tTextInst.endValue());
    }

    @Test
    void testMinValue() throws SQLException {
        TTextInst tTextInst = new TTextInst("abcd@2019-09-08 06:04:32+02");
        assertEquals("abcd", tTextInst.minValue());
    }

    @Test
    void testMaxValue() throws SQLException {
        TTextInst tTextInst = new TTextInst("this is a test@2019-09-08 06:04:32+02");
        assertEquals("this is a test", tTextInst.maxValue());
    }

    @Test
    void testValueAtTimestampNull() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TTextInst tTextInst = new TTextInst("what@2019-09-08 06:10:32+02");
        assertNull(tTextInst.valueAtTimestamp(timestamp));
    }

    @Test
    void testValueAtTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime timestamp = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TTextInst tTextInst = new TTextInst("which@2019-09-08 06:04:32+02");
        assertEquals(tTextInst.getValue(), tTextInst.valueAtTimestamp(timestamp));
    }

    @Test
    void testGetTimestamp() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);
        TTextInst tTextInst = new TTextInst("who@2019-09-08 06:04:32+02");
        assertEquals(expectedDate, tTextInst.getTimestamp());
    }
}
