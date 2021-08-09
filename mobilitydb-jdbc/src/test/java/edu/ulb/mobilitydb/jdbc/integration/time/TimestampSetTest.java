package edu.ulb.mobilitydb.jdbc.integration.time;

import edu.ulb.mobilitydb.jdbc.integration.BaseIntegrationTest;
import edu.ulb.mobilitydb.jdbc.time.PeriodSet;
import edu.ulb.mobilitydb.jdbc.time.TimestampSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

class TimestampSetTest extends BaseIntegrationTest {
    @ParameterizedTest
    @ValueSource(strings = {
        "{2019-09-08 00:00:00+01}",
        "{2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01, 2019-09-11 00:00:00+01}"
    })
    void testConstructor(String value) throws SQLException {
        TimestampSet set = new TimestampSet(value);

        PreparedStatement insertStatement = con.prepareStatement("INSERT INTO tbl_timestampset  (timetype) VALUES (?);");
        insertStatement.setObject(1, set);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement("SELECT timetype FROM tbl_timestampset WHERE timetype=?;");
        readStatement.setObject(1, set);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            TimestampSet retrievedSet = (TimestampSet) rs.getObject(1);
            assertEquals(set, retrievedSet);
        } else {
            fail("Period set was not retried.");
        }

        readStatement.close();
    }
}
