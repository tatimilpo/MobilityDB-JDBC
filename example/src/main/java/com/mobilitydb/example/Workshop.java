package com.mobilitydb.example;

import org.apache.commons.lang3.time.StopWatch;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Based on https://github.com/MobilityDB/MobilityDB-workshop
 */
public class Workshop {
    public static void main(String[] args) {
        try {
            Connection con = Common.createConnection(25433, "DanishAIS");
            StopWatch watch = new StopWatch();
            watch.start();
            createSchema(con);
            System.out.println("createSchema: " + watch.getTime());
            loadFromCSV(con);
            System.out.println("loadFromCSV: " + watch.getTime());
            cleanupData(con);
            System.out.println("cleanupData " + watch.getTime());
            filterData(con);
            System.out.println("filterData " + watch.getTime());
            loadShipData(con);
            System.out.println("loadShipData " + watch.getTime());
            watch.stop();
            con.close();
        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(Playground.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    private static void createSchema(Connection con) throws SQLException {
        String inputTable  = "CREATE TABLE IF NOT EXISTS AISInput( " +
                "T timestamp, " +
                "TypeOfMobile varchar(50), " +
                "MMSI integer, " +
                "Latitude float, " +
                "Longitude float, " +
                "navigationalStatus varchar(50), " +
                "ROT float, " +
                "SOG float, " +
                "COG float, " +
                "Heading integer, " +
                "IMO varchar(50), " +
                "Callsign varchar(50), " +
                "Name varchar(100), " +
                "ShipType varchar(50), " +
                "CargoType varchar(100), " +
                "Width float, " +
                "Length float, " +
                "TypeOfPositionFixingDevice varchar(50), " +
                "Draught float, " +
                "Destination varchar(50), " +
                "ETA varchar(50), " +
                "DataSourceType varchar(50), " +
                "SizeA float, " +
                "SizeB float, " +
                "SizeC float, " +
                "SizeD float, " +
                "Geom geometry(Point, 4326) " +
                "); ";
        
        String shipsTable = "CREATE TABLE IF NOT EXISTS Ships( " +
                "MMSI integer, " +
                "Trip tgeompoint, " +
                "SOG tfloat, " +
                "COG tfloat" +
                "); ";

        Statement inputStatement = con.createStatement();
        inputStatement.execute(inputTable);
        inputStatement.close();

        Statement shipsStatement = con.createStatement();
        shipsStatement.execute(shipsTable);
        shipsStatement.close();
    }

    private static void loadFromCSV(Connection con) throws SQLException {
        String sql = "COPY AISInput(T, TypeOfMobile, MMSI, Latitude, Longitude, NavigationalStatus, " +
                "ROT, SOG, COG, Heading, IMO, CallSign, Name, ShipType, CargoType, Width, Length, " +
                "TypeOfPositionFixingDevice, Draught, Destination, ETA, DataSourceType, " +
                "SizeA, SizeB, SizeC, SizeD, Geom) " +
                "FROM '/workshopData/ais_data/ais.csv' DELIMITER ',' CSV HEADER;";

        Statement st = con.createStatement();
        st.execute(sql);
        st.close();
    }

    private static void cleanupData(Connection con) throws SQLException {
        String sql = "UPDATE AISInput SET " +
                "NavigationalStatus = CASE NavigationalStatus WHEN 'Unknown value' THEN NULL END, " +
                "IMO = CASE IMO WHEN 'Unknown' THEN NULL END, " +
                "ShipType = CASE ShipType WHEN 'Undefined' THEN NULL END, " +
                "TypeOfPositionFixingDevice = CASE TypeOfPositionFixingDevice " +
                "WHEN 'Undefined' THEN NULL END, " +
                "Geom = ST_SetSRID( ST_MakePoint( Longitude, Latitude ), 4326); ";

        Statement st = con.createStatement();
        st.execute(sql);
        st.close();
    }

    private static void filterData(Connection con) throws SQLException {
        String sql = "CREATE TABLE AISInputFiltered AS " +
                "SELECT DISTINCT ON(MMSI,T) * " +
                "FROM AISInput " +
                "WHERE Longitude BETWEEN -16.1 and 32.88 AND Latitude BETWEEN 40.18 AND 84.17; ";

        Statement st = con.createStatement();
        st.execute(sql);
        st.close();
    }

    private static void loadShipData(Connection con) {

    }
}
