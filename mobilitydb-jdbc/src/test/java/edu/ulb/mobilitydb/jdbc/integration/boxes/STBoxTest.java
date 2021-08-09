package edu.ulb.mobilitydb.jdbc.integration.boxes;

import edu.ulb.mobilitydb.jdbc.boxes.STBox;
import edu.ulb.mobilitydb.jdbc.integration.BaseIntegrationTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class STBoxTest extends BaseIntegrationTest {

    @ParameterizedTest
    @ValueSource(strings = {
        ""
    })
    void testConstructor(String value) throws SQLException {
        STBox stBox = new STBox(value);

        PreparedStatement insertStatement = con.prepareStatement("INSERT INTO tbl_stbox(boxtype) VALUES (?);");
        insertStatement.setObject(1, stBox);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement("SELECT boxtype FROM tbl_stbox WHERE boxtype=?;");
        readStatement.setObject(1, stBox);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            STBox retrievedTBox = (STBox) rs.getObject(1);
            assertEquals(stBox, retrievedTBox);
        } else {
            fail("STBox was not retrieved.");
        }

        readStatement.close();
    }
}
