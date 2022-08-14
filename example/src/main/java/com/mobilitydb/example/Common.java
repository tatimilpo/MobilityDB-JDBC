package com.mobilitydb.example;

import com.mobilitydb.jdbc.DataTypeHandler;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Common {
    private static final String url = "jdbc:postgresql://localhost:%s/%s";
    private static final String user = "docker";
    private static final String password = "docker";

    public static Connection createConnection(int port, String dataBase) throws SQLException {
        Connection con = DriverManager.getConnection(String.format(url, port, dataBase),
                user,
                password);
        // Add the JDBC extension object
        PGConnection pgconn = (PGConnection) con;
        // Register MobilityDB types
        DataTypeHandler.INSTANCE.registerTypes(pgconn);
        return con;
    }
}
