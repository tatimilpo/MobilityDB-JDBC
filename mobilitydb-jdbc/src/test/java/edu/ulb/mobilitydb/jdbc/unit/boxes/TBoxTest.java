package edu.ulb.mobilitydb.jdbc.unit.boxes;

import edu.ulb.mobilitydb.jdbc.boxes.TBox;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;

import static org.junit.jupiter.api.Assertions.*;

class TBoxTest {

    @Test
    void testConstructor() throws SQLException {
        String value = "TBOX((50.0, 2021-07-08 06:04:32+02), (40.0, 2021-07-09 11:02:00+02))";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedTmin = OffsetDateTime.of(2021,7, 8,
                6, 4, 32, 0, tz);
        OffsetDateTime expectedTmax = OffsetDateTime.of(2021, 7, 9,
                11, 2, 0, 0, tz);
        TBox tbox = new TBox(value);
        TBox other = new TBox(50.0, expectedTmin, 40.0, expectedTmax);

        assertAll("Constructor with all arguments",
                () -> assertEquals(expectedTmin, tbox.getTmin()),
                () -> assertEquals(expectedTmax, tbox.getTmax()),
                () -> assertEquals(other.getValue(), tbox.getValue())
        );
    }

    @Test
    void testConstructorOnlyX() throws SQLException {
        String value = "TBOX((3.0, ), (7.0, ))";
        TBox tbox = new TBox(value);
        TBox other = new TBox(3.0, 7.0);

        assertAll("Constructor with only x coordinates",
                () -> assertEquals(other.getXmin(), tbox.getXmin()),
                () -> assertEquals(other.getXmax(), tbox.getXmax()),
                () -> assertEquals(other.getValue(), tbox.getValue())
        );
    }

    @Test
    void testConstructorOnlyT() throws SQLException {
        String value = "TBOX((, 2021-07-08 06:04:32+02), (, 2021-07-09 11:02:00+02))";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime expectedTmin = OffsetDateTime.of(2021,7, 8,
                6, 4, 32, 0, tz);
        OffsetDateTime expectedTmax = OffsetDateTime.of(2021, 7, 9,
                11, 2, 0, 0, tz);
        TBox tbox = new TBox(value);
        TBox other = new TBox(expectedTmin, expectedTmax);

        assertAll("Constructor with only x coordinates",
                () -> assertEquals(expectedTmin, tbox.getTmin()),
                () -> assertEquals(expectedTmax, tbox.getTmax()),
                () -> assertEquals(other.getValue(), tbox.getValue())
        );
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "",
            "(20, 2021-09-08 00:00:00+01, 2021-09-08 00:00:00+01)",
            "(20, 2021-09-08 00:00:00+01, 40)",
            "( 2021-09-08 00:00:00+01, 60, 2021-09-08 00:00:00+01)",
    })
    void testSetValueInvalidValue(String value) throws SQLException {
        TBox tBox = new TBox();
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tBox.setValue(value)
        );
        assertTrue(thrown.getMessage().contains("Could not parse TBox value, invalid number of arguments."));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "((10.0, 2019-09-08 00:00:00+02), (, 2019-09-10 00:00:00+02))",
            "TBOX((10.0, 2019-04-18 00:03:00+02), (, 2019-04-20 00:04:00+02))",
            "((10.0,), (,))",
            "TBOX((8.0,), (,))"
    })
    void testSetValueMissingXmax(String value) throws SQLException {
        TBox tBox = new TBox();
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tBox.setValue(value)
        );
        assertTrue(thrown.getMessage().contains("Xmax should have a value."));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "((, 2019-09-08 00:00:00+02), (32.0, 2019-09-10 00:00:00+02))",
            "TBOX((, 2019-09-08 00:00:00+02), (15, 2019-09-10 00:00:00+02))",
            "((,), (33.0, ))",
            "TBOX((,), (87, ))"
    })
    void testSetValueMissingXmin(String value) throws SQLException {
        TBox tBox = new TBox();
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tBox.setValue(value)
        );
        assertTrue(thrown.getMessage().contains("Xmin should have a value."));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "((10.0, 2019-09-08 00:00:00+02), (22.0, ))",
            "TBOX((10.0, 2019-04-18 00:03:00+02), (22.0, ))",
            "((10.0,2019-04-18 00:11:02+02), (18,))",
            "TBOX((8.0,2019-04-19 00:03:15+02), (28,))"
    })
    void testSetValueMissingTmax(String value) throws SQLException {
        TBox tBox = new TBox();
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tBox.setValue(value)
        );
        assertTrue(thrown.getMessage().contains("Tmax should have a value."));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "((10.0,), (22.0, 2019-09-08 00:00:00+02))",
            "TBOX((10.0,), (22.0, 2019-04-18 00:03:00+02))",
            "((10.0,), (18, 2019-04-18 00:11:02+02))",
            "TBOX((8.0,), (28,2019-04-19 00:03:15+02))"
    })
    void testSetValueMissingTmin(String value) throws SQLException {
        TBox tBox = new TBox();
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> tBox.setValue(value)
        );
        assertTrue(thrown.getMessage().contains("Tmin should have a value."));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "TBOX((, -09-08 00:00:00+01), (, 2021-07-09 11:02:00+02))",
            "TBOX((4, 2021-07-09 11:02:00+02), (9, 2021-09-08))",
            "((4, 2021-07-0912:02:00+02), (9, 2021-07-09 12:09:08+02))"
    })
    void testSetValueInvalidDateFormat(String value) throws SQLException {
        TBox tBox = new TBox();
        DateTimeParseException thrown = assertThrows(
                DateTimeParseException.class,
                () -> tBox.setValue(value)
        );
        assertTrue(thrown.getMessage().contains("could not be parsed"));
    }

    @Test
    void testEmptyEquals() {
        TBox tBoxA = new TBox();
        TBox tBoxB = new TBox();
        assertEquals(tBoxA, tBoxB);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "TBOX((30.0, 2021-07-13 06:04:32+02), (98.0, 2021-07-16 11:02:00+02))",
            "TBOX((3.0, ), (7.0, ))",
            "TBOX((, 2021-07-11 08:04:32+02), (, 2021-07-12 11:02:00+02))"
    })
    void testEquals(String value) throws SQLException {
        TBox tBoxA = new TBox(value);
        TBox tBoxB = new TBox(value);
        assertEquals(tBoxA, tBoxB);
    }

    @Test
    void testEqualsOnlyX() throws SQLException {
        TBox tBoxA = new TBox(3.0, 7.0);
        TBox tBoxB = new TBox(3.0, 7.0);

        assertAll("Equals on x coordinate",
                () -> assertEquals(tBoxA.getXmin(), tBoxB.getXmin()),
                () -> assertEquals(tBoxA.getXmax(), tBoxB.getXmax()),
                () -> assertEquals(tBoxA.getValue(), tBoxB.getValue())
        );
    }

    @Test
    void testEqualsOnlyT() throws SQLException {
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime tmin = OffsetDateTime.of(2021,7, 8,
                6, 4, 32, 0, tz);
        OffsetDateTime tmax = OffsetDateTime.of(2021, 7, 9,
                11, 2, 0, 0, tz);
        TBox tBoxA = new TBox(tmin, tmax);
        TBox tBoxB = new TBox(tmin, tmax);

        assertAll("Equals on time dimension",
                () -> assertEquals(tBoxA.getXmin(), tBoxB.getXmin()),
                () -> assertEquals(tBoxA.getXmax(), tBoxB.getXmax()),
                () -> assertEquals(tBoxA.getValue(), tBoxB.getValue())
        );
    }
}
