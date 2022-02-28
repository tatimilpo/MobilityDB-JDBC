package edu.ulb.mobilitydb.jdbc.integration.tfloat;

import edu.ulb.mobilitydb.jdbc.integration.BaseIntegrationTest;
import edu.ulb.mobilitydb.jdbc.tfloat.TFloat;
import edu.ulb.mobilitydb.jdbc.tfloat.TFloatSeq;
import edu.ulb.mobilitydb.jdbc.tfloat.TFloatSeqSet;
import edu.ulb.mobilitydb.jdbc.tint.TInt;
import edu.ulb.mobilitydb.jdbc.tint.TIntSeq;
import edu.ulb.mobilitydb.jdbc.tint.TIntSeqSet;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TFloatSeqSetTest extends BaseIntegrationTest {
    @Test
    void testStringConstructor() throws Exception {
        String value = "{[100.2@2019-09-08 08:00:00+02, 20.9@2019-09-09 08:00:00+02,20.4@2019-09-10 08:00:00+02]," +
                "[15.6@2019-09-11 08:00:00+02, 30.7@2019-09-12 08:00:00+02]}";

        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(value);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tfloat (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tFloatSeqSet.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tfloat WHERE temporaltype=?;");
        readStatement.setObject(1, tFloatSeqSet.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TFloat retrievedTFloat = (TFloat) rs.getObject(1);
            assertEquals(tFloatSeqSet, new TFloatSeqSet(retrievedTFloat));
        } else {
            fail("TInt was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testStringsConstructor() throws Exception {
        String[] values = new String[] {"[100.2@2019-09-08 08:00:00+02, 20.9@2019-09-09 08:00:00+02]",
                "[15.6@2019-09-11 08:00:00+02, 30.7@2019-09-12 08:00:00+02]"};

        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(values);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tfloat (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tFloatSeqSet.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tfloat WHERE temporaltype=?;");
        readStatement.setObject(1, tFloatSeqSet.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TFloat retrievedTFloat = (TFloat) rs.getObject(1);
            assertEquals(tFloatSeqSet, new TFloatSeqSet(retrievedTFloat));
        } else {
            fail("TInt was not retrieved.");
        }

        readStatement.close();
    }

    @Test
    void testSequencesConstructor() throws Exception {
        TFloatSeq[] values = new TFloatSeq[]{ new TFloatSeq("[100.2@2019-09-08 08:00:00+02, " +
                "20.9@2019-09-09 08:00:00+02,20.4@2019-09-10 08:00:00+02]"),
                new TFloatSeq("[15.6@2019-09-11 08:00:00+02, 30.7@2019-09-12 08:00:00+02]")};

        TFloatSeqSet tFloatSeqSet = new TFloatSeqSet(values);

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO tbl_tfloat (temporaltype) VALUES (?);");
        insertStatement.setObject(1, tFloatSeqSet.getDataType());
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement(
                "SELECT temporaltype FROM tbl_tfloat WHERE temporaltype=?;");
        readStatement.setObject(1, tFloatSeqSet.getDataType());
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TFloat retrievedTFloat = (TFloat) rs.getObject(1);
            assertEquals(tFloatSeqSet, new TFloatSeqSet(retrievedTFloat));
        } else {
            fail("TInt was not retrieved.");
        }

        readStatement.close();
    }
}
