package com.mobilitydb.jdbc.unit.tpoint;

import com.mobilitydb.jdbc.temporal.TemporalValue;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.tpoint.TPoint;
import org.junit.jupiter.api.Test;
import org.postgis.Point;

import java.sql.SQLException;
import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.*;

class TPointTest {
    @Test
    void testGetSingleTemporalValue_valid() throws SQLException {
        String value = "Point(1 1)@2019-09-08 06:04:32+02";
        TemporalValue<Point> temporalValue = TPoint.getSingleTemporalValue(value);
        assertEquals(new Point(1, 1), temporalValue.getValue());
    }

    @Test
    void testGetSingleTemporalValue_null() {
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> TPoint.getSingleTemporalValue(null)
        );

        assertTrue(thrown.getMessage().contains("Value cannot be null."));
    }

    @Test
    void testGetSingleTemporalValue_invalidFormat() {
        String value = "random string";
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> TPoint.getSingleTemporalValue(value)
        );

        assertTrue(thrown.getMessage().contains("is not a valid temporal value."));
    }

    @Test
    void testGetSingleTemporalValue_invalidType() {
        String value = "POLYGON(1 1)@2019-09-08 06:04:32+02";
        SQLException thrown = assertThrows(
                SQLException.class,
                () -> TPoint.getSingleTemporalValue(value)
        );

        assertTrue(thrown.getMessage().contains("is an invalid Postgis geometry type for temporal point."));
    }
}
