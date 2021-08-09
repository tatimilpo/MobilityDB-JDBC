package edu.ulb.mobilitydb.example;

import edu.ulb.mobilitydb.jdbc.time.Period;
import edu.ulb.mobilitydb.jdbc.time.PeriodSet;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Time {
    public static void main(String[] args) {
        try {
            Connection con = Common.createConnection();
            System.out.println("Period");
            testReadPeriod(con);
            System.out.println("Period Set");
            testReadPeriodSet(con);
            con.close();
        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(Time.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    public static void testReadPeriod(Connection con) throws SQLException {
        String command = "SELECT period ('2000-01-01','2000-01-02');";
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(command);

        if (rs.next()) {
            Period a = (Period)rs.getObject(1);
            System.out.format("Value retrieved: %s%n", a.toString());
            System.out.format("Is lower inclusive: %b%n", a.isLowerInclusive());
            System.out.format("Lower date: %s%n", a.getLower().toString());
            System.out.format("Is upper inclusive: %b%n", a.isUpperInclusive());
            System.out.format("Upper date: %s%n", a.getUpper().toString());
        }

        st.close();
    }

    public static void testReadPeriodSet(Connection con) throws SQLException {
        String command = "SELECT PeriodSet '{[2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01], [2019-09-11 00:00:00+01, 2019-09-12 00:00:00+01]}';";
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(command);

        if (rs.next()) {
            PeriodSet pgo = (PeriodSet) rs.getObject(1);
            System.out.format("Type: %s%n", pgo.getType());
            System.out.format("Value: %s", pgo.getValue());
        }

        st.close();
    }
}
