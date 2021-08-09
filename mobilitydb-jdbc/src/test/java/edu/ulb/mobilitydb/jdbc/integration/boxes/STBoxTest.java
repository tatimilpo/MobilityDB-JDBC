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
        "STBOX ((1.0, 2.0), (1.0, 2.0))",
        "STBOX Z((1.0, 2.0, 3.0), (1.0, 2.0, 3.0))",
        "STBOX T((1.0, 2.0, 2001-01-03 00:00:00+01), (1.0, 2.0, 2001-01-03 00:00:00+01))",
        "STBOX ZT((1.0, 2.0, 3.0, 2001-01-04 00:00:00+01), (1.0, 2.0, 3.0, 2001-01-04 00:00:00+01))",
        "STBOX T(, 2001-01-03 00:00:00+01), (, 2001-01-03 00:00:00+01))",
        "GEODSTBOX((11.0, 12.0, 13.0), (11.0, 12.0, 13.0))",
        "GEODSTBOX T((1.0, 2.0, 3.0, 2001-01-03 00:00:00+01), (1.0, 2.0, 3.0, 2001-01-04 00:00:00+01))",
        "GEODSTBOX T((, 2001-01-03 00:00:00+01), (, 2001-01-03 00:00:00+01))",
        "SRID=5676;STBOX T((1.0, 2.0, 2001-01-04 00:00:00+01), (1.0, 2.0, 2001-01-04 00:00:00+01))",
        "SRID=4326;GEODSTBOX((1.0, 2.0, 3.0), (1.0, 2.0, 3.0))"
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
