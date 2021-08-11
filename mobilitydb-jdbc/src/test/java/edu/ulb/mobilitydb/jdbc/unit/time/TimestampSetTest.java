package edu.ulb.mobilitydb.jdbc.unit.time;

import edu.ulb.mobilitydb.jdbc.time.Period;
import edu.ulb.mobilitydb.jdbc.time.PeriodSet;
import edu.ulb.mobilitydb.jdbc.time.TimestampSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class TimestampSetTest {
    static Stream<Arguments> notEqualsTimestampSets() {
        return Stream.of(
                arguments("{2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01, 2019-09-11 00:00:00+01}",
                        "{2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01}"),
                arguments("{2019-09-08 00:00:00+01}",
                        "{2019-09-07 00:00:00+01}"),
                arguments("{2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01, 2019-09-11 00:00:00+01}",
                        "{2019-09-08 00:00:00+02, 2019-09-10 00:00:00+02, 2019-09-11 00:00:00+02}")
        );
    }

    @Test
    void testEquals() throws SQLException {
        String value = "{2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01, 2019-09-11 00:00:00+01}";
        TimestampSet setA = new TimestampSet(value);
        TimestampSet setB = new TimestampSet(value);
        assertEquals(setA, setB);
    }

    @Test
    void testEqualsTimezone() throws SQLException {
        String valueA = "{2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01, 2019-09-11 00:00:00+01}";
        String valueB = "{2019-09-07 19:00:00-04, 2019-09-10 03:00:00+04, 2019-09-10 21:00:00-02}";
        TimestampSet setA = new TimestampSet(valueA);
        TimestampSet setB = new TimestampSet(valueB);
        assertEquals(setA, setB);
    }

    @ParameterizedTest
    @MethodSource("notEqualsTimestampSets")
    void testNotEquals(String valueA, String valueB) throws SQLException {
        TimestampSet setA = new TimestampSet(valueA);
        TimestampSet setB = new TimestampSet(valueB);
        assertNotEquals(setA, setB);
    }

    @Test
    void testEmptyEquals() {
        TimestampSet setA = new TimestampSet();
        TimestampSet setB = new TimestampSet();
        assertEquals(setA, setB);
    }

    @Test
    void testGetValue() throws SQLException {
        String value = "{2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01, 2019-09-11 00:00:00+01}";
        TimestampSet setA = new TimestampSet(value);
        TimestampSet setB = new TimestampSet(setA.getValue());
        assertEquals(setA.getValue(), setB.getValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01, 2019-09-11 00:00:00+01",
        "{2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01, 2019-09-11 00:00:00+01",
        "2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01, 2019-09-11 00:00:00+01}"
    })
    void testConstructorInvalidValue(String value) {
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> new TimestampSet(value)
        );
        assertEquals("Could not parse timestamp set value.", thrown.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "{2019-09-08 00:00:00+01, 2019-09-07 00:00:00+01, 2019-09-11 00:00:00+01}",
        "{2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01, 2019-09-10 00:00:00+01}"
    })
    void testConstructorInvalidTimeStamps(String value) {
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> new TimestampSet(value)
        );
        assertEquals("The timestamps of a timestamp set must be increasing.", thrown.getMessage());
    }

    @Test
    void testListConstructor() throws SQLException {
        OffsetDateTime now = OffsetDateTime.now();
        TimestampSet set = new TimestampSet(
                now,
                now.plusDays(1),
                now.plusDays(2)
        );
        assertEquals(3, set.numTimestamps());
        assertEquals(now, set.startTimestamp());
        assertEquals(now.plusDays(1), set.timestampN(1));
        assertEquals(now.plusDays(2), set.endTimestamp());
    }

    @Test
    void testGetTimeSpan() throws SQLException {
        OffsetDateTime now = OffsetDateTime.now();
        TimestampSet set = new TimestampSet(
                now,
                now.plusDays(1),
                now.plusDays(2)
        );
        assertEquals(Duration.ofDays(2), set.getTimeSpan());
    }

    @Test
    void testGetPeriod() throws SQLException {
        OffsetDateTime now = OffsetDateTime.now();
        TimestampSet set = new TimestampSet(
                now,
                now.plusDays(1),
                now.plusDays(2)
        );
        Period expected = new Period(now, now.plusDays(2), true, true);
        assertEquals(expected, set.getPeriod());
    }

    @Test
    void testShift() throws SQLException {
        OffsetDateTime now = OffsetDateTime.now();
        TimestampSet set = new TimestampSet(
                now,
                now.plusDays(1),
                now.plusDays(2)
        ).shift(Duration.ofDays(1));

        assertEquals(3, set.numTimestamps());
        assertEquals(now.plusDays(1), set.startTimestamp());
        assertEquals(now.plusDays(2), set.timestampN(1));
        assertEquals(now.plusDays(3), set.endTimestamp());
    }
}
