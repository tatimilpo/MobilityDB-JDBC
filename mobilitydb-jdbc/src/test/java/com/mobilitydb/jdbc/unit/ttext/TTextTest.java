package com.mobilitydb.jdbc.unit.ttext;

import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.ttext.*;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TTextTest {
    @Test
    void testConstructorTTextInst() throws SQLException {
        String value = "ABCDE@2019-09-08 06:04:32+02";
        TText tText = new TText(value);
        assertEquals(TemporalType.TEMPORAL_INSTANT, tText.getTemporalType());
        assertTrue(tText.getTemporal() instanceof TTextInst);
        TText another = new TText(tText.getTemporal());
        assertEquals(tText, another);
    }

    @Test
    void testConstructorTTextInstSet() throws SQLException {
        String value = "{ABCDE@2001-01-01 08:00:00+02, FGHIJ@2001-01-03 08:00:00+02}";
        TText tText = new TText(value);
        assertEquals(TemporalType.TEMPORAL_INSTANT_SET, tText.getTemporalType());
        assertTrue(tText.getTemporal() instanceof TTextInstSet);
        TText another = new TText(tText.getTemporal());
        assertEquals(tText, another);
    }

    @Test
    void testConstructorTTextSeq() throws SQLException {
        String value = "(abcd@2001-01-01 08:00:00+02, efghi@2001-01-03 08:00:00+02]";
        TText tText = new TText(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE, tText.getTemporalType());
        assertTrue(tText.getTemporal() instanceof TTextSeq);
        TText another = new TText(tText.getTemporal());
        assertEquals(tText, another);
    }

    @Test
    void testConstructorTTextSeqSet() throws SQLException {
        String value = "{(abcd@2001-01-01 08:00:00+02, efgh@2001-01-03 08:00:00+02], " +
                "[ijkl@2001-01-04 08:00:00+02, mnop@2001-01-05 08:00:00+02, qrst@2001-01-06 08:00:00+02]}";
        TText tText = new TText(value);
        assertEquals(TemporalType.TEMPORAL_SEQUENCE_SET, tText.getTemporalType());
        assertTrue(tText.getTemporal() instanceof TTextSeqSet);
        TText another = new TText(tText.getTemporal());
        assertEquals(tText, another);
    }
}
