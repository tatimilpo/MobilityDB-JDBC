package com.mobilitydb.example;

import org.postgresql.util.PGobject;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Playground {
    public static void main(String[] args) {
        try {
            Connection con = Common.createConnection(25432, "mobilitydb");
            // Modify SELECT for desired type
            String command = "SELECT PeriodSet '{[2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01], [2019-09-11 00:00:00+01, 2019-09-12 00:00:00+01]}';";
            Statement st = con.createStatement();
            ResultSet rs = st.executeQuery(command);

            if (rs.next()) {
                PGobject pgo = (PGobject) rs.getObject(1);
                System.out.format("Type: %s%n", pgo.getType());
                System.out.format("Value: %s", pgo.getValue());
            }

            st.close();
            con.close();
        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(Playground.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }
}
