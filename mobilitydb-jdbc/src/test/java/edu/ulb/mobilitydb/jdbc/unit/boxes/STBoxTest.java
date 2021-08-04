package edu.ulb.mobilitydb.jdbc.unit.boxes;

import edu.ulb.mobilitydb.jdbc.boxes.STBox;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
}
