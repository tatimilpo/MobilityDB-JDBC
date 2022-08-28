package com.mobilitydb.jdbc.unit.tint;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tint.*;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TIntTest {
    @Test
    void testConstructorTIntInst() throws SQLException {
        String value = "10@2019-09-08 06:04:32+02";
        TInt tInt = new TInt(value);
        assertEquals(TemporalType.TEMPORAL_INSTANT, tInt.getTemporalType());
        assertTrue(tInt.getTemporal() instanceof TIntInst);
        TInt another = new TInt(tInt.getTemporal());
        assertEquals(tInt, another);
    }

    @Test
    void testConstructorTIntInstSet() throws SQLException {
        String value = "{1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02}";
        TInt tInt = new TInt(value);
        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tInt.getTemporalType());
        assertTrue(tInt.getTemporal() instanceof TIntInstSet);
        TInt another = new TInt(tInt.getTemporal());
        assertEquals(tInt, another);
    }

    @Test
    void testConstructorTIntSeq() throws SQLException {
        String value = "(1@2001-01-01 08:00:00+02, 2@2001-01-03 08:00:00+02]";
        TInt tInt = new TInt(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE, tInt.getTemporalType());
        assertTrue(tInt.getTemporal() instanceof TIntSeq);
        TInt another = new TInt(tInt.getTemporal());
        assertEquals(tInt, another);
    }

    @Test
    void testConstructorTIntSeqSet() throws SQLException {
        String value = "{[1@2001-01-01 08:00:00+02, 1@2001-01-03 08:00:00+02), " +
                "[2@2001-01-04 08:00:00+02, 3@2001-01-05 08:00:00+02, 3@2001-01-06 08:00:00+02]}";
        TInt tInt = new TInt(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE_SET, tInt.getTemporalType());
        assertTrue(tInt.getTemporal() instanceof TIntSeqSet);
        TInt another = new TInt(tInt.getTemporal());
        assertEquals(tInt, another);
    }
}
