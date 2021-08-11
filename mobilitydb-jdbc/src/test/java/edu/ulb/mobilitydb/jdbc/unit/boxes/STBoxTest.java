package edu.ulb.mobilitydb.jdbc.unit.boxes;

import edu.ulb.mobilitydb.jdbc.boxes.STBox;
import edu.ulb.mobilitydb.jdbc.boxes.STBoxBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class STBoxTest {

    static Stream<Arguments> stboxInvalidTimeProvider() {
        return Stream.of(
            arguments(null, null, null, OffsetDateTime.now(), null, null, null, null, 0,  false),
            arguments(null, null, null, null, null, null, null, OffsetDateTime.now(), 0,  false)
        );
    }

    static Stream<Arguments> stboxInvalidXYCoordinatesProvider() {
        return Stream.of(
            arguments(null, 1.0, null, OffsetDateTime.now(), null, null, null, OffsetDateTime.now(), 0,  false),
            arguments(1.0, 2.0, null, OffsetDateTime.now(), 3.0, null, null, OffsetDateTime.now(), 0,  false),
            arguments(null, 1.0, null, null, null, null, null, null, 1234,  false),
            arguments(1.0, 2.0, null, null, 3.0, null, null, null, 3456,  false),
            arguments(null, 1.0, 8.0, OffsetDateTime.now(), 7.0, 1.0, null, OffsetDateTime.now(), 5643,  true),
            arguments(1.0, 2.0, null, OffsetDateTime.now(), null, 6.0, 35.0, OffsetDateTime.now(), 2446,  true),
            arguments(null, 12.0, 27.0, null, 9.2, 13.6, null, null, 8654,  true),
            arguments(1.0, 2.0, null, null, 3.0, null, 10.0, null, 4566,  true)
        );
    }

    static Stream<Arguments> stboxInvalidZCoordinateProvider() {
        return Stream.of(
            arguments(5.0, 1.0, 8.0, OffsetDateTime.now(), 7.0, 1.0, null, OffsetDateTime.now(), 0,  false),
            arguments(1.0, 2.0, null, OffsetDateTime.now(), 3.0, 6.0, 35.0, OffsetDateTime.now(), 0,  false),
            arguments(9.0, 12.0, 27.0, null, 9.2, 13.6, null, null, 0,  false),
            arguments(1.0, 2.0, null, null, 3.0, 8.0, 10.0, null, 0,  false)
        );
    }

    @Test
    void testConstructorTime() throws SQLException {
        String value = "STBOX T(, 2001-01-03 00:00:00+03), (, 2001-01-03 00:00:00+03))";
        ZoneOffset tz = ZoneOffset.of("+03:00");
        OffsetDateTime ttmin = OffsetDateTime.of(2001,1, 3,
                0, 0, 0, 0, tz);
        OffsetDateTime ttmax = OffsetDateTime.of(2001, 1, 3,
                0, 0, 0, 0, tz);
        STBox stbox = new STBox(value);
        STBox other = new STBoxBuilder()
                .setTime(ttmin,ttmax)
                .build();

        assertAll("Constructor with time dimension",
                () -> assertEquals(other.getTmin(), stbox.getTmin()),
                () -> assertEquals(other.getTmax(), stbox.getTmax()),
                () -> assertEquals(other.isGeodetic(), stbox.isGeodetic()),
                () -> assertEquals(other.getValue(), stbox.getValue())
        );
    }

    @Test
    void testConstructorXYandTime() throws SQLException {
        String value = "SRID=5676;STBOX T((1.0, 2.0, 2001-01-04 00:00:00+01), (1.0, 2.0, 2001-01-04 00:00:00+01))";
        ZoneOffset tz = ZoneOffset.of("+01:00");
        OffsetDateTime ttmin = OffsetDateTime.of(2001,1, 4,
                0, 0, 0, 0, tz);
        OffsetDateTime ttmax = OffsetDateTime.of(2001, 1, 4,
                0, 0, 0, 0, tz);
        STBox stbox = new STBox(value);
        STBox other = new STBoxBuilder()
                .setXYCoordinates(1.0,2.0,1.0,2.0)
                .setTime(ttmin,ttmax)
                .setSrid(5676)
                .build();

        assertAll("Constructor with XY coordinates and time dimension",
                () -> assertEquals(other.getXmin(), stbox.getXmin()),
                () -> assertEquals(other.getXmax(), stbox.getXmax()),
                () -> assertEquals(other.getYmin(), stbox.getYmin()),
                () -> assertEquals(other.getYmax(), stbox.getYmax()),
                () -> assertEquals(other.getTmin(), stbox.getTmin()),
                () -> assertEquals(other.getTmax(), stbox.getTmax()),
                () -> assertEquals(other.getSrid(), stbox.getSrid()),
                () -> assertEquals(other.getValue(), stbox.getValue())
        );
    }

    @Test
    void testConstructorXY() throws SQLException {
        String value = "STBOX ((1.0, 2.0), (1.0, 2.0))";
        STBox stbox = new STBox(value);
        STBox other = new STBoxBuilder()
                .setXYCoordinates(1.0,2.0,1.0,2.0)
                .build();

        assertAll("Constructor with XY coordinates",
            () -> assertEquals(other.getXmin(), stbox.getXmin()),
            () -> assertEquals(other.getXmax(), stbox.getXmax()),
            () -> assertEquals(other.getYmin(), stbox.getYmin()),
            () -> assertEquals(other.getYmax(), stbox.getYmax()),
            () -> assertEquals(other.getValue(), stbox.getValue())
        );
    }

    @Test
    void testConstructorXYZ() throws SQLException {
        String value = "STBOX Z((1.0, 2.0, 3.0), (1.0, 2.0, 3.0))";
        STBox stbox = new STBox(value);
        STBox other = new STBoxBuilder()
                .setXYZCoordinates(1.0,2.0,3.0,1.0,2.0,3.0)
                .build();

        assertAll("Constructor with XYZ coordinates",
            () -> assertEquals(other.getXmin(), stbox.getXmin()),
            () -> assertEquals(other.getXmax(), stbox.getXmax()),
            () -> assertEquals(other.getYmin(), stbox.getYmin()),
            () -> assertEquals(other.getYmax(), stbox.getYmax()),
            () -> assertEquals(other.getZmin(), stbox.getZmin()),
            () -> assertEquals(other.getZmax(), stbox.getZmax()),
            () -> assertEquals(other.getValue(), stbox.getValue())
        );
    }

    @Test
    void testConstructorXYZAndTimeDimension() throws SQLException {
        String value = "STBOX ZT((1.0, 2.0, 3.0, 2001-01-04 00:00:00+01), (1.0, 2.0, 3.0, 2001-01-04 00:00:00+01))";
        ZoneOffset tz = ZoneOffset.of("+01:00");
        OffsetDateTime ttmin = OffsetDateTime.of(2001,1, 4,
                0, 0, 0, 0, tz);
        OffsetDateTime ttmax = OffsetDateTime.of(2001, 1, 4,
                0, 0, 0, 0, tz);
        STBox stbox = new STBox(value);
        STBox other = new STBoxBuilder()
                .setXYZCoordinates(1.0,2.0,3.0,1.0,2.0,3.0)
                .setTime(ttmin,ttmax)
                .build();

        assertAll("Constructor with XY coordinates and time dimension",
            () -> assertEquals(other.getXmin(), stbox.getXmin()),
            () -> assertEquals(other.getXmax(), stbox.getXmax()),
            () -> assertEquals(other.getYmin(), stbox.getYmin()),
            () -> assertEquals(other.getYmax(), stbox.getYmax()),
            () -> assertEquals(other.getTmin(), stbox.getTmin()),
            () -> assertEquals(other.getTmax(), stbox.getTmax()),
            () -> assertEquals(other.getZmin(), stbox.getZmin()),
            () -> assertEquals(other.getZmax(), stbox.getZmax()),
            () -> assertEquals(other.getValue(), stbox.getValue())
        );
    }

    @Test
    void testConstructorTimeGeodetic() throws SQLException {
        String value = "GEODSTBOX T((, 2021-01-03 00:00:00+01), (, 2021-01-03 00:00:00+01))";
        ZoneOffset tz = ZoneOffset.of("+01:00");
        OffsetDateTime ttmin = OffsetDateTime.of(2021,1, 3,
                0, 0, 0, 0, tz);
        OffsetDateTime ttmax = OffsetDateTime.of(2021, 1, 3,
                0, 0, 0, 0, tz);
        STBox stbox = new STBox(value);
        STBox other = new STBoxBuilder()
                .setTime(ttmin,ttmax)
                .isGeodetic(true)
                .build();

        assertAll("Constructor with time dimension and geodetic",
                () -> assertEquals(other.getTmin(), stbox.getTmin()),
                () -> assertEquals(other.getTmax(), stbox.getTmax()),
                () -> assertEquals(other.isGeodetic(), stbox.isGeodetic()),
                () -> assertEquals(other.getValue(), stbox.getValue())
        );
    }

    @Test
    void testConstructorXYZGeodetic() throws SQLException {
        String value = "SRID=4326;GEODSTBOX((11.0, 12.0, 13.0), (11.0, 12.0, 13.0))";
        STBox stbox = new STBox(value);
        STBox other = new STBoxBuilder()
                .setXYZCoordinates(11.0,12.0,13.0,11.0,12.0,13.0)
                .isGeodetic(true)
                .setSrid(4326)
                .build();

        assertAll("Constructor with XYZ coordinates and geodetic",
            () -> assertEquals(other.getXmin(), stbox.getXmin()),
            () -> assertEquals(other.getXmax(), stbox.getXmax()),
            () -> assertEquals(other.getYmin(), stbox.getYmin()),
            () -> assertEquals(other.getYmax(), stbox.getYmax()),
            () -> assertEquals(other.getZmin(), stbox.getZmin()),
            () -> assertEquals(other.getZmax(), stbox.getZmax()),
            () -> assertEquals(other.isGeodetic(), stbox.isGeodetic()),
            () -> assertEquals(other.getSrid(), stbox.getSrid()),
            () -> assertEquals(other.getValue(), stbox.getValue())
        );
    }


    @Test
    void testConstructorXYZAndTimeDimensionGeodetic() throws SQLException {
        String value = "GEODSTBOX T((1.0, 2.0, 3.0, 2001-01-03 00:00:00+01), (1.0, 2.0, 3.0, 2001-01-04 00:00:00+01))";
        ZoneOffset tz = ZoneOffset.of("+01:00");
        OffsetDateTime ttmin = OffsetDateTime.of(2001,1, 3,
                0, 0, 0, 0, tz);
        OffsetDateTime ttmax = OffsetDateTime.of(2001, 1, 4,
                0, 0, 0, 0, tz);
        STBox stbox = new STBox(value);
        STBox other = new STBoxBuilder()
                .setXYZCoordinates(1.0,2.0,3.0,1.0,2.0,3.0)
                .setTime(ttmin,ttmax)
                .isGeodetic(true)
                .build();

        assertAll("Constructor with XY coordinates and time dimension and geodetic",
                () -> assertEquals(other.getXmin(), stbox.getXmin()),
                () -> assertEquals(other.getXmax(), stbox.getXmax()),
                () -> assertEquals(other.getYmin(), stbox.getYmin()),
                () -> assertEquals(other.getYmax(), stbox.getYmax()),
                () -> assertEquals(other.getTmin(), stbox.getTmin()),
                () -> assertEquals(other.getTmax(), stbox.getTmax()),
                () -> assertEquals(other.getZmin(), stbox.getZmin()),
                () -> assertEquals(other.getZmax(), stbox.getZmax()),
                () -> assertEquals(other.isGeodetic(), stbox.isGeodetic()),
                () -> assertEquals(other.getValue(), stbox.getValue())
        );
    }

    @Test
    void testBuilderXY() throws SQLException {
        STBoxBuilder builder = new STBoxBuilder();
        STBox test = builder.setXYCoordinates(1.0,2.0,3.0,4.0)
                .build();
        assertEquals(1.0, test.getXmin());
        assertEquals(3.0, test.getXmax());
        assertEquals(2.0, test.getYmin());
        assertEquals(4.0, test.getYmax());
    }

    @Test
    void testBuilderXYAndSrid() throws SQLException {
        STBoxBuilder builder = new STBoxBuilder();
        STBox stBox = builder.setXYCoordinates(1.0,2.0,3.0,4.0)
                .setSrid(12345)
                .build();
        assertEquals(1.0, stBox.getXmin());
        assertEquals(3.0, stBox.getXmax());
        assertEquals(2.0, stBox.getYmin());
        assertEquals(4.0, stBox.getYmax());
        assertEquals(12345, stBox.getSrid());
    }

    @Test
    void testBuilderXYZAndSrid() throws SQLException {
        STBoxBuilder builder = new STBoxBuilder();
        STBox stBox = builder.setXYZCoordinates(1.0,2.0,3.0,4.0, 5.0, 6.0)
                .setSrid(12345)
                .build();
        assertEquals(1.0, stBox.getXmin());
        assertEquals(4.0, stBox.getXmax());
        assertEquals(2.0, stBox.getYmin());
        assertEquals(5.0, stBox.getYmax());
        assertEquals(3.0, stBox.getZmin());
        assertEquals(6.0, stBox.getZmax());
        assertEquals(12345, stBox.getSrid());
    }

    @Test
    void testBuilderXYAndTime() throws SQLException {
        STBoxBuilder builder = new STBoxBuilder();
        ZoneOffset tz = ZoneOffset.of("+04:00");
        OffsetDateTime tmin = OffsetDateTime.of(2021,4, 8,
                5, 32, 10, 0, tz);
        OffsetDateTime tmax = OffsetDateTime.of(2021, 4, 9,
                10, 17, 21, 0, tz);
        STBox stBox = builder.setXYCoordinates(1.0,2.0,3.0,4.0)
                .setTime(tmin, tmax)
                .setSrid(12345)
                .build();

        assertEquals(1.0, stBox.getXmin());
        assertEquals(3.0, stBox.getXmax());
        assertEquals(2.0, stBox.getYmin());
        assertEquals(4.0, stBox.getYmax());
        assertEquals(tmin, stBox.getTmin());
        assertEquals(tmax, stBox.getTmax());
    }

    @Test
    void testBuilderXYZAndTime() throws SQLException {
        STBoxBuilder builder = new STBoxBuilder();
        ZoneOffset tz = ZoneOffset.of("+04:00");
        OffsetDateTime tmin = OffsetDateTime.of(2021,4, 8,
                5, 32, 10, 0, tz);
        OffsetDateTime tmax = OffsetDateTime.of(2021, 4, 9,
                10, 17, 21, 0, tz);
        STBox stBox = builder.setXYZCoordinates(1.0,2.0,3.0,4.0, 5.0, 6.0)
                .setTime(tmin, tmax)
                .isGeodetic(true)
                .build();
        assertAll("Testing builder with XYZ coordinates and geodetic",
                () -> assertEquals(1.0, stBox.getXmin()),
                () -> assertEquals(4.0, stBox.getXmax()),
                () -> assertEquals(2.0, stBox.getYmin()),
                () -> assertEquals(5.0, stBox.getYmax()),
                () -> assertEquals(3.0, stBox.getZmin()),
                () -> assertEquals(6.0, stBox.getZmax()),
                () -> assertEquals(tmin, stBox.getTmin()),
                () -> assertEquals(tmax, stBox.getTmax()),
                () -> assertTrue(stBox.isGeodetic())
        );
    }

    @Test
    void testBuilderException() {
        Throwable exceptionThrown = assertThrows(SQLException.class, () -> {
            new STBoxBuilder()
                .setSrid(12345)
                .isGeodetic(true)
                .build();
        });
        assertTrue(exceptionThrown.getMessage().contains("Could not parse STBox value"));
    }

    @Test
    void testEmptyEquals() {
        STBox stBoxA = new STBox();
        STBox stBoxB = new STBox();
        assertEquals(stBoxA, stBoxB);
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "STBOX ((1.0, 2.0), (1.0, 2.0))",
        "STBOX Z((1.0, 2.0, 3.0), (1.0, 2.0, 3.0))",
        "STBOX T((1.0, 2.0, 2001-01-03 00:00:00+01), (1.0, 2.0, 2001-01-03 00:00:00+01))",
        "STBOX ZT((1.0, 2.0, 3.0, 2001-01-04 00:00:00+01), (1.0, 2.0, 3.0, 2001-01-04 00:00:00+01))",
        "STBOX T(, 2001-01-03 00:00:00+01), (, 2001-01-03 00:00:00+01))",
        "GEODSTBOX((11.0, 12.0, 13.0), (11.0, 12.0, 13.0))",
        "GEODSTBOX T((1.0, 2.0, 3.0, 2001-01-03 00:00:00+01), (1.0, 2.0, 3.0, 2001-01-04 00:00:00+01))",
        "GEODSTBOX T((, 2001-01-03 00:00:00+01), (, 2001-01-03 00:00:00+01))",
        "SRID=5676;STBOX T((1.0, 2.0, 2001-01-04 00:00:00+01), (1.0, 2.0, 2001-01-04 00:00:00+01))",
        "SRID=4326;GEODSTBOX((1.0, 2.0, 3.0), (1.0, 2.0, 3.0))"
    })
    void testEquals(String value) throws SQLException {
        STBox stBoxA = new STBox();
        STBox stBoxB = new STBox();
        assertEquals(stBoxA, stBoxB);
    }

    @ParameterizedTest
    @MethodSource("stboxInvalidTimeProvider")
    void testInvalidTime(Double xmin, Double ymin, Double zmin, OffsetDateTime tmin,
                         Double xmax, Double ymax, Double zmax, OffsetDateTime tmax,
                         int srid, boolean isGeodetic) {
        Throwable exceptionThrown = assertThrows(SQLException.class, () -> {
            new STBox(xmin, ymin, zmin, tmin, xmax, ymax, zmax, tmax, srid, isGeodetic);
        });
        assertTrue(exceptionThrown.getMessage().contains("Both tmin and tmax should have a value"));
    }

    @ParameterizedTest
    @MethodSource("stboxInvalidXYCoordinatesProvider")
    void testInvalidXYCoordinates(Double xmin, Double ymin, Double zmin, OffsetDateTime tmin,
                                  Double xmax, Double ymax, Double zmax, OffsetDateTime tmax,
                                  int srid, boolean isGeodetic) {
        Throwable exceptionThrown = assertThrows(SQLException.class, () -> {
            new STBox(xmin, ymin, zmin, tmin, xmax, ymax, zmax, tmax, srid, isGeodetic);
        });
        assertTrue(exceptionThrown.getMessage().contains("Both x and y coordinates should have a value"));
    }

    @ParameterizedTest
    @MethodSource("stboxInvalidZCoordinateProvider")
    void testInvalidZCoordinate(Double xmin, Double ymin, Double zmin, OffsetDateTime tmin,
                                  Double xmax, Double ymax, Double zmax, OffsetDateTime tmax,
                                  int srid, boolean isGeodetic) {
        Throwable exceptionThrown = assertThrows(SQLException.class, () -> {
            new STBox(xmin, ymin, zmin, tmin, xmax, ymax, zmax, tmax, srid, isGeodetic);
        });
        assertTrue(exceptionThrown.getMessage().contains("Both zmax and zmin should have a value"));
    }

    @Test
    void testInvalidArguments() {
        Throwable exceptionThrown = assertThrows(SQLException.class, () -> {
            new STBox(null, null, null, null, null, null, null, null,
                    0,  true);
        });
        assertTrue(exceptionThrown.getMessage().contains("Could not parse STBox value, invalid number of arguments"));
    }

}
