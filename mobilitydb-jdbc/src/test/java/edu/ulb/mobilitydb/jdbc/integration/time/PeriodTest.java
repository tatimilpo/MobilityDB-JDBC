package edu.ulb.mobilitydb.jdbc.integration.time;

import edu.ulb.mobilitydb.jdbc.integration.BaseIntegrationTest;
import edu.ulb.mobilitydb.jdbc.time.Period;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.*;

class PeriodTest extends BaseIntegrationTest {
    
    @ParameterizedTest
    @ValueSource(strings = {
        "[2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01]",
        "[2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01)",
        "(2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01]",
        "(2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01)",
    })
    void testConstructor(String value) throws SQLException {
        Period period = new Period(value);

        PreparedStatement insertStatement = con.prepareStatement("INSERT INTO tbl_period (timetype) VALUES (?);");
        insertStatement.setObject(1, period);
        insertStatement.execute();
        insertStatement.close();

        PreparedStatement readStatement = con.prepareStatement("SELECT timetype FROM tbl_period WHERE timetype=?;");
        readStatement.setObject(1, period);
        ResultSet rs = readStatement.executeQuery();

        if (rs.next()) {
            Period retrievedPeriod = (Period) rs.getObject(1);
            assertEquals(period, retrievedPeriod);
        } else {
            fail("Period was not retrieved.");
        }

        readStatement.close();
    }
}
