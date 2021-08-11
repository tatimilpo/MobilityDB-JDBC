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
            arguments(null, 1.0, null, null, null, null, null, null, 0,  false),
            arguments(1.0, 2.0, null, null, 3.0, null, null, null, 0,  false)
        );
    }

    @Test
    void testStringConstructor() throws SQLException {
        String value = "STBOX T(, 2021-01-03 09:09:00+01), (, 2021-01-03 10:10:00+01))";
        ZoneOffset tz = ZoneOffset.of("+01:00");
        OffsetDateTime expectedTmin = OffsetDateTime.of(2021,1, 3,
                9, 9, 0, 0, tz);
        OffsetDateTime expectedTmax = OffsetDateTime.of(2021, 1, 3,
                10, 10, 0, 0, tz);
        STBox stbox = new STBox(value);

        assertEquals(expectedTmin, stbox.getTmin());
        assertEquals(expectedTmax, stbox.getTmax());
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

}
