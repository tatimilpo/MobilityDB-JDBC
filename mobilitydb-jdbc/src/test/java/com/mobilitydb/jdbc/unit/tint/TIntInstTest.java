package com.mobilitydb.jdbc.unit.tint;


import com.mobilitydb.jdbc.tint.TIntInst;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TIntInstTest {
    @Test
    void testConstructor() throws SQLException {
        String value = "10@2019-09-08 06:04:32+02";
        ZoneOffset tz = ZoneOffset.of("+02:00");
        OffsetDateTime otherDate = OffsetDateTime.of(2019,9, 8,
                6, 4, 32, 0, tz);

        TIntInst tIntInst = new TIntInst(value);
        TIntInst other = new TIntInst(10, otherDate);

        assertEquals(other.getTemporalValue(), tIntInst.getTemporalValue());
    }
}
