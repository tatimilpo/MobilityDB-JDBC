package com.mobilitydb.jdbc.integration.tpoint.tgeog;

import com.mobilitydb.jdbc.integration.BaseIntegrationTest;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPoint;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointSeq;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPointSeqSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.fail;

class TGeogPointSeqSetTest extends BaseIntegrationTest {
    @ParameterizedTest
    @ValueSource(strings = {
            "{[SRID=4326;POINT(1 1)@2001-01-01 08:00:00+02, SRID=4326;POINT(1 1)@2001-01-03 08:00:00+02), " +
                    "[SRID=4326;POINT(1 1)@2001-01-04 08:00:00+02, SRID=4326;POINT(1 1)@2001-01-06 08:00:00+02]}",
            "{[SRID=4326;010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "SRID=4326;010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                    "[SRID=4326;010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "SRID=4326;010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}",
            "{[POINT(1 1)@2001-01-01 08:00:00+02, POINT(1 1)@2001-01-03 08:00:00+02), " +
                    "[POINT(1 1)@2001-01-04 08:00:00+02, POINT(1 1)@2001-01-06 08:00:00+02]}",
            "{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                    "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}",
            "Interp=Stepwise;{[SRID=4326;POINT(1 1)@2001-01-01 08:00:00+02, SRID=4326;POINT(1 1)@2001-01-03 08:00:00+02), " +
                    "[SRID=4326;POINT(1 1)@2001-01-04 08:00:00+02, SRID=4326;POINT(1 1)@2001-01-06 08:00:00+02]}",
            "Interp=Stepwise;{[SRID=4326;010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "SRID=4326;010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                    "[SRID=4326;010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "SRID=4326;010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}",
            "Interp=Stepwise;{[POINT(1 1)@2001-01-01 08:00:00+02, POINT(1 1)@2001-01-03 08:00:00+02), " +
                    "[POINT(1 1)@2001-01-04 08:00:00+02, POINT(1 1)@2001-01-06 08:00:00+02]}",
            "Interp=Stepwise;{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                    "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}",
            "SRID=4326;{[SRID=4326;POINT(1 1)@2001-01-01 08:00:00+02, SRID=4326;POINT(1 1)@2001-01-03 08:00:00+02), " +
                    "[SRID=4326;POINT(1 1)@2001-01-04 08:00:00+02, SRID=4326;POINT(1 1)@2001-01-06 08:00:00+02]}",
            "SRID=4326;{[SRID=4326;010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "SRID=4326;010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                    "[SRID=4326;010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "SRID=4326;010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}",
            "SRID=4326;{[POINT(1 1)@2001-01-01 08:00:00+02, POINT(1 1)@2001-01-03 08:00:00+02), " +
                    "[POINT(1 1)@2001-01-04 08:00:00+02, POINT(1 1)@2001-01-06 08:00:00+02]}",
            "SRID=4326;{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                    "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}",
            "SRID=4326;Interp=Stepwise;{[SRID=4326;POINT(1 1)@2001-01-01 08:00:00+02, SRID=4326;POINT(1 1)@2001-01-03 08:00:00+02), " +
                    "[SRID=4326;POINT(1 1)@2001-01-04 08:00:00+02, SRID=4326;POINT(1 1)@2001-01-06 08:00:00+02]}",
            "SRID=4326;Interp=Stepwise;{[SRID=4326;010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "SRID=4326;010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                    "[SRID=4326;010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "SRID=4326;010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}",
            "SRID=4326;Interp=Stepwise;{[POINT(1 1)@2001-01-01 08:00:00+02, POINT(1 1)@2001-01-03 08:00:00+02), " +
                    "[POINT(1 1)@2001-01-04 08:00:00+02, POINT(1 1)@2001-01-06 08:00:00+02]}",
            "SRID=4326;Interp=Stepwise;{[010100000000000000000000000000000000000000@2001-01-01 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-03 08:00:00+02), " +
                    "[010100000000000000000000000000000000000000@2001-01-04 08:00:00+02, " +
                    "010100000000000000000000000000000000000000@2001-01-06 08:00:00+02]}"
    })
    void testStringConstructor(String value) throws Exception {
        TGeogPoint tGeogPoint = new TGeogPoint(value);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tGeogpoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeogPoint);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tGeogpoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeogPoint);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeogPoint retrievedTGeogPoint = (TGeogPoint) rs.getObject(1);
            Assertions.assertEquals(tGeogPoint.getTemporal(), retrievedTGeogPoint.getTemporal());
        } else {
            fail("TGeogPoint was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringsConstructor() throws Exception {
        String[] values = new String[] {
            "[SRID=4326;Point(10 10)@2001-01-01 08:00:00+02, SRID=4326;Point(10 15)@2001-01-03 08:00:00+02)",
            "[SRID=4326;Point(20 10)@2001-01-04 08:00:00+02, SRID=4326;Point(20 20)@2001-01-06 08:00:00+02]"
        };

        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(false, values);
        TGeogPoint tGeogPoint = new TGeogPoint(tGeogPointSeqSet);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tGeogpoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeogPoint);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tGeogpoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeogPoint);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeogPoint retrievedTGeogPoint = (TGeogPoint) rs.getObject(1);
            Assertions.assertEquals(tGeogPoint.getTemporal(), retrievedTGeogPoint.getTemporal());
        } else {
            fail("TGeogPoint was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testSequencesConstructor() throws Exception {
        TGeogPointSeq[] values = new TGeogPointSeq[]{
            new TGeogPointSeq("[SRID=4326;Point(10 15)@2001-01-01 08:00:00+02, SRID=4326;Point(10 20)@2001-01-03 08:00:00+02)"),
            new TGeogPointSeq("[SRID=4326;Point(20 10)@2001-01-04 08:00:00+02, SRID=4326;Point(20 20)@2001-01-06 08:00:00+02]")
        };

        TGeogPointSeqSet tGeogPointSeqSet = new TGeogPointSeqSet(false, values);
        TGeogPoint tGeogPoint = new TGeogPoint(tGeogPointSeqSet);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tGeogpoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeogPoint);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tGeogpoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeogPoint);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeogPoint retrievedTGeogPoint = (TGeogPoint) rs.getObject(1);
            Assertions.assertEquals(tGeogPoint.getTemporal(), retrievedTGeogPoint.getTemporal());
        } else {
            fail("TGeogPoint was not retrieved.");
        }

        readStatement.close();
    }
}
