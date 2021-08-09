package edu.ulb.mobilitydb.jdbc.unit.boxes;

import edu.ulb.mobilitydb.jdbc.boxes.STBox;
import edu.ulb.mobilitydb.jdbc.boxes.STBoxBuilder;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

class STBoxTest {

    @Test
    void testConstructor() throws SQLException {
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
                .hasTimeDimension(true)
                .setSrid(12345)
                .build();
        assertEquals(1.0, stBox.getXmin());
        assertEquals(3.0, stBox.getXmax());
        assertEquals(2.0, stBox.getYmin());
        assertEquals(4.0, stBox.getYmax());
        assertTrue(stBox.isDimT());
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

}
