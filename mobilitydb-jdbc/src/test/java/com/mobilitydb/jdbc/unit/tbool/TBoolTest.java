package com.mobilitydb.jdbc.unit.tbool;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tbool.*;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TBoolTest {
    @Test
    void testConstructorTBoolInst() throws SQLException {
        String value = "true@2019-09-08 06:04:32+02";
        TBool tBool = new TBool(value);
        assertEquals(TemporalType.TEMPORAL_INSTANT, tBool.getTemporalType());
        assertTrue(tBool.getTemporal() instanceof TBoolInst);
        TBool another = new TBool(tBool.getTemporal());
        assertEquals(tBool, another);
    }

    @Test
    void testConstructorTBoolInstSet() throws SQLException {
        String value = "{true@2001-01-01 08:00:00+02,false@2001-01-03 08:00:00+02}";
        TBool tBool = new TBool(value);
        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tBool.getTemporalType());
        assertTrue(tBool.getTemporal() instanceof TBoolInstSet);
        TBool another = new TBool(tBool.getTemporal());
        assertEquals(tBool, another);
    }

    @Test
    void testConstructorTBoolSeq() throws SQLException {
        String value = "(true@2001-01-01 08:00:00+02, false@2001-01-03 08:00:00+02]";
        TBool tBool = new TBool(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE, tBool.getTemporalType());
        assertTrue(tBool.getTemporal() instanceof TBoolSeq);
        TBool another = new TBool(tBool.getTemporal());
        assertEquals(tBool, another);
    }

    @Test
    void testConstructorTBoolSeqSet() throws SQLException {
        String value = "{[true@2001-01-01 08:00:00+02, true@2001-01-03 08:00:00+02), " +
                "[true@2001-01-04 08:00:00+02, false@2001-01-05 08:00:00+02, false@2001-01-06 08:00:00+02]}";
        TBool tBool = new TBool(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE_SET, tBool.getTemporalType());
        assertTrue(tBool.getTemporal() instanceof TBoolSeqSet);
        TBool another = new TBool(tBool.getTemporal());
        assertEquals(tBool, another);
    }
}
