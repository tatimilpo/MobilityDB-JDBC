package edu.ulb.mobilitydb.jdbc.unit.time;

import edu.ulb.mobilitydb.jdbc.time.Period;
import edu.ulb.mobilitydb.jdbc.time.PeriodSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class PeriodSetTest {
    static Stream<Arguments> periodSetTimezoneProvider() {
        return Stream.of(
                arguments("{[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01]}",
                        "{[2021-09-08 01:00:00+02, 2021-09-10 02:00:00+03]}"),
                arguments("{[2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)}",
                        "{[2021-09-08 03:00:00+04, 2021-09-10 04:00:00+05)}"),
                arguments("{(2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01]}",
                        "{(2021-09-07 22:00:00-01, 2021-09-09 21:00:00-02]}"),
                arguments("{(2021-09-08 00:00:00+01, 2021-09-10 00:00:00+01)}",
                        "{(2021-09-07 20:00:00-03, 2021-09-09 19:00:00-04)}")
        );
    }

    @Test
    void testEquals() throws SQLException {
        String value = "{[2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01], [2019-09-11 00:00:00+01, 2019-09-12 00:00:00+01]}";
        PeriodSet periodSetA = new PeriodSet(value);
        PeriodSet periodSetB = new PeriodSet(value);
        assertEquals(periodSetA, periodSetB);
    }

    @ParameterizedTest
    @MethodSource("periodSetTimezoneProvider")
    void testEqualsTimeZone(String first, String second) throws SQLException {
        PeriodSet periodSetA = new PeriodSet(first);
        PeriodSet periodSetB = new PeriodSet(second);
        assertEquals(periodSetA, periodSetB);
    }

    @Test
    void testNotEquals() throws SQLException {
        String valueA = "{[2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01], [2019-09-11 00:00:00+01, 2019-09-12 00:00:00+01]}";
        String valueB = "{[2019-09-07 00:00:00+01, 2019-09-10 00:00:00+01], [2019-09-11 00:00:00+01, 2019-09-12 00:00:00+01]}";
        PeriodSet periodSetA = new PeriodSet(valueA);
        PeriodSet periodSetB = new PeriodSet(valueB);
        assertNotEquals(periodSetA, periodSetB);
    }

    @Test
    void testNotEqualsTimeZone() throws SQLException {
        String valueA = "{[2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01], [2019-09-11 00:00:00+01, 2019-09-12 00:00:00+01]}";
        String valueB = "{[2019-09-08 00:00:00+02, 2019-09-10 00:00:00+01], [2019-09-11 00:00:00+01, 2019-09-12 00:00:00+01]}";
        PeriodSet periodSetA = new PeriodSet(valueA);
        PeriodSet periodSetB = new PeriodSet(valueB);
        assertNotEquals(periodSetA, periodSetB);
    }

    @Test
    void testEmptyEquals() {
        PeriodSet periodSetA = new PeriodSet();
        PeriodSet periodSetB = new PeriodSet();
        assertEquals(periodSetA, periodSetB);
    }

    @Test
    void testGetValue() throws SQLException {
        String value = "{[2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01], [2019-09-11 00:00:00+01, 2019-09-12 00:00:00+01]}";
        PeriodSet periodSetA = new PeriodSet(value);
        PeriodSet periodSetB = new PeriodSet(periodSetA.getValue());
        assertEquals(periodSetA.getValue(), periodSetB.getValue());
    }

    @Test
    void testPeriodListConstructor() throws SQLException {
        String expectedValue = "{[2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01], [2019-09-11 00:00:00+01, 2019-09-12 00:00:00+01]}";
        PeriodSet periodSet = new PeriodSet(
                new Period("[2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01]"),
                new Period("[2019-09-11 00:00:00+01, 2019-09-12 00:00:00+01]")
        );
        assertEquals(expectedValue, periodSet.getValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "[2019-09-08 00:00:00+01, 2019-09-12 00:00:00+01], [2019-09-11 00:00:00+01, 2019-09-12 00:00:00+01]",
            "{[2019-09-08 00:00:00+01, 2019-09-12 00:00:00+01], [2019-09-11 00:00:00+01, 2019-09-12 00:00:00+01]",
            "[2019-09-08 00:00:00+01, 2019-09-12 00:00:00+01], [2019-09-11 00:00:00+01, 2019-09-12 00:00:00+01]}"
    })
    void testConstructorInvalidValue(String value) {
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> new PeriodSet(value)
        );
        assertEquals("Could not parse period set value.", thrown.getMessage());
    }

    @Test
    void testPeriodWithoutValue() throws SQLException {
        Period[][] tests = {
                new Period[]{new Period()},
                new Period[]{new Period("[2019-09-08 00:00:00+01, 2019-09-12 00:00:00+01]"), new Period()}
        };

        for (Period[] periods : tests) {
            SQLException thrown = assertThrows(
                    SQLException.class,
                    () -> new PeriodSet(periods)
            );
            assertEquals("All periods should have a value.", thrown.getMessage());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{[2019-09-08 00:00:00+01, 2019-09-12 00:00:00+01], [2019-09-11 00:00:00+01, 2019-09-12 00:00:00+01]}",
            "{[2019-09-08 00:00:00+01, 2019-09-11 00:00:00+01], [2019-09-11 00:00:00+01, 2019-09-12 00:00:00+01]}"
    })
    void testInvalidPeriods(String value) {
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> new PeriodSet(value)
        );
        assertEquals("The periods of a period set cannot overlap.", thrown.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{[2019-09-08 00:00:00+01, 2019-09-11 00:00:00+01], (2019-09-11 00:00:00+01, 2019-09-12 00:00:00+01]}",
            "{[2019-09-08 00:00:00+01, 2019-09-11 00:00:00+01), [2019-09-11 00:00:00+01, 2019-09-12 00:00:00+01]}"
    })
    void testValidPeriods(String value) throws SQLException {
        PeriodSet periodSet = new PeriodSet(value);
        assertEquals(value, periodSet.getValue());
    }
}
