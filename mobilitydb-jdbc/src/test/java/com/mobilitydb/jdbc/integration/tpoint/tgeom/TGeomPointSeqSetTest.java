package com.mobilitydb.jdbc.integration.tpoint.tgeom;

import com.mobilitydb.jdbc.integration.BaseIntegrationTest;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPoint;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointSeq;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointSeqSet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.fail;

class TGeomPointSeqSetTest extends BaseIntegrationTest {
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
        TGeomPoint tGeomPoint = new TGeomPoint(value);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tGeompoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeomPoint);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tGeompoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeomPoint);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeomPoint retrievedTGeomPoint = (TGeomPoint) rs.getObject(1);
            Assertions.assertEquals(tGeomPoint.getTemporal(), retrievedTGeomPoint.getTemporal());
        } else {
            fail("TGeom was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringsConstructor() throws Exception {
        String[] values = new String[] {
                "[SRID=4326;Point(10 10)@2001-01-01 08:00:00+02, SRID=4326;Point(10 15)@2001-01-03 08:00:00+02)",
                "[SRID=4326;Point(20 10)@2001-01-04 08:00:00+02, SRID=4326;Point(20 20)@2001-01-06 08:00:00+02]"
        };

        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(false, values);
        TGeomPoint tGeomPoint = new TGeomPoint(tGeomPointSeqSet);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tGeompoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeomPoint);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tGeompoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeomPoint);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeomPoint retrievedTGeomPoint = (TGeomPoint) rs.getObject(1);
            Assertions.assertEquals(tGeomPoint.getTemporal(), retrievedTGeomPoint.getTemporal());
        } else {
            fail("TGeomPoint was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testSequencesConstructor() throws Exception {
        TGeomPointSeq[] values = new TGeomPointSeq[]{
                new TGeomPointSeq("[SRID=4326;Point(10 15)@2001-01-01 08:00:00+02, SRID=4326;Point(10 20)@2001-01-03 08:00:00+02)"),
                new TGeomPointSeq("[SRID=4326;Point(20 10)@2001-01-04 08:00:00+02, SRID=4326;Point(20 20)@2001-01-06 08:00:00+02]")
        };

        TGeomPointSeqSet tGeomPointSeqSet = new TGeomPointSeqSet(false, values);
        TGeomPoint tGeomPoint = new TGeomPoint(tGeomPointSeqSet);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tGeompoint (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tGeomPoint);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tGeompoint WHERE temporaltype=?;");
        readStatement.setObject(1, tGeomPoint);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TGeomPoint retrievedTGeomPoint = (TGeomPoint) rs.getObject(1);
            Assertions.assertEquals(tGeomPoint.getTemporal(), retrievedTGeomPoint.getTemporal());
        } else {
            fail("TGeomPoint was not retrieved.");
        }

        readStatement.close();
    }
}
