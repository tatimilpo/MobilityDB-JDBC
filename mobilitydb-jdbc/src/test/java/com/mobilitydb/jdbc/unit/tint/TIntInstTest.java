package com.mobilitydb.jdbc.unit.tint;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tint.TIntInst;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

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

    @ParameterizedTest
    @ValueSource(strings = {
            "254@2019-09-08 06:04:32+02",
            "659@2019-09-08 06:04:32+02"
    })
    void testInstSetType(String value) throws SQLException {
        TIntInst tIntInst = new TIntInst(value);

        assertEquals(TemporalType.TEMPORAL_INSTANT, tIntInst.getTemporalType());
    }

    @Test
    void testInvalidEmptyValue() {
        SQLException thrown = assertThrows(
            SQLException.class,
            () -> {
                TIntInst tIntInst = new TIntInst("");
            }
        );
        assertTrue(thrown.getMessage().contains("Value cannot be empty."));
    }
}
