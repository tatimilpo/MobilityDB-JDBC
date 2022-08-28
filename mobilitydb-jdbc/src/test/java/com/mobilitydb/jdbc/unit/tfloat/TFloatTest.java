package com.mobilitydb.jdbc.unit.tfloat;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tfloat.*;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TFloatTest {
    @Test
    void testConstructorTFloatInst() throws SQLException {
        String value = "10.5@2019-09-08 06:04:32+02";
        TFloat tFloat = new TFloat(value);
        assertEquals(TemporalType.TEMPORAL_INSTANT, tFloat.getTemporalType());
        assertTrue(tFloat.getTemporal() instanceof TFloatInst);
        TFloat another = new TFloat(tFloat.getTemporal());
        assertEquals(tFloat, another);
    }

    @Test
    void testConstructorTFloatInstSet() throws SQLException {
        String value = "{1.3@2001-01-01 08:00:00+02, 2.6@2001-01-03 08:00:00+02}";
        TFloat tFloat = new TFloat(value);
        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tFloat.getTemporalType());
        assertTrue(tFloat.getTemporal() instanceof TFloatInstSet);
        TFloat another = new TFloat(tFloat.getTemporal());
        assertEquals(tFloat, another);
    }

    @Test
    void testConstructorTFloatSeq() throws SQLException {
        String value = "[16.98@2001-01-01 08:00:00+02, 42.36@2001-01-03 08:00:00+02)";
        TFloat tFloat = new TFloat(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE, tFloat.getTemporalType());
        assertTrue(tFloat.getTemporal() instanceof TFloatSeq);
        TFloat another = new TFloat(tFloat.getTemporal());
        assertEquals(tFloat, another);
    }

    @Test
    void testConstructorTFloatSeqSet() throws SQLException {
        String value = "{[1@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02), " +
                "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}";
        TFloat tFloat = new TFloat(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE_SET, tFloat.getTemporalType());
        assertTrue(tFloat.getTemporal() instanceof TFloatSeqSet);
        TFloat another = new TFloat(tFloat.getTemporal());
        assertEquals(tFloat, another);
    }
}
