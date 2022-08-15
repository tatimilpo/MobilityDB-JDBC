package com.mobilitydb.example;

import com.mobilitydb.jdbc.tfloat.TFloat;
import com.mobilitydb.jdbc.tfloat.TFloatInst;
import com.mobilitydb.jdbc.tfloat.TFloatSeq;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPoint;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointInst;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPointSeq;
import org.apache.commons.lang3.time.StopWatch;
import org.postgis.PGgeometry;
import org.postgis.Point;

import java.sql.*;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Based on https://github.com/MobilityDB/MobilityDB-workshop
 */
public class Workshop {
    public static void main(String[] args) {
        try {
            Connection con = Common.createConnection(25433, "DanishAIS");
            createSchema(con);
            loadFromCSV(con);
            cleanupData(con);
            filterData(con);
            loadShipData(con);
            setTrajectory(con);
            con.close();
        } catch (SQLException ex) {
            Logger lgr = Logger.getLogger(Playground.class.getName());
            lgr.log(Level.SEVERE, ex.getMessage(), ex);
        }
    }

    /**
     * Creates the required tables.
     * See Workshop documentation 1.4 Preparing the Database
     * @param con - SQL connection
     * @throws SQLException - when the create fails
     */
    private static void createSchema(Connection con) throws SQLException {
        StopWatch watch = new StopWatch();
        watch.start();
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
                "COG tfloat, " +
                "Traj geometry" +
                "); ";

        Statement inputStatement = con.createStatement();
        inputStatement.execute(inputTable);
        inputStatement.close();

        Statement shipsStatement = con.createStatement();
        shipsStatement.execute(shipsTable);
        shipsStatement.close();
        watch.stop();
        System.out.println("createSchema: " + watch.getTime() + "ms");
    }

    /**
     * Load the workshop data to AISInput table
     * See Workshop documentation 1.5 Loading the Data
     * @param con - SQL connection
     * @throws SQLException - If the copy fails
     */
    private static void loadFromCSV(Connection con) throws SQLException {
        StopWatch watch = new StopWatch();
        watch.start();
        String sql = "COPY AISInput(T, TypeOfMobile, MMSI, Latitude, Longitude, NavigationalStatus, " +
                "ROT, SOG, COG, Heading, IMO, CallSign, Name, ShipType, CargoType, Width, Length, " +
                "TypeOfPositionFixingDevice, Draught, Destination, ETA, DataSourceType, " +
                "SizeA, SizeB, SizeC, SizeD, Geom) " +
                "FROM '/workshopData/ais_data/ais.csv' DELIMITER ',' CSV HEADER;";

        Statement st = con.createStatement();
        st.execute(sql);
        st.close();
        watch.stop();
        System.out.println("loadFromCSV: " + watch.getTime() + "ms");
    }

    /**
     * Cleanup AISInput table
     * See Workshop documentation 1.5 Loading the Data
     * @param con - SQL connection
     * @throws SQLException - If the update fails
     */
    private static void cleanupData(Connection con) throws SQLException {
        StopWatch watch = new StopWatch();
        watch.start();
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
        watch.stop();
        System.out.println("cleanupData: " + watch.getTime() + "ms");
    }

    /**
     * Filter the data that it is outside a defined window and then saves it to AISInputFiltered
     * See Workshop documentation 1.5 Loading the Data
     * @param con - SQL connection
     * @throws SQLException - If filter fails
     */
    private static void filterData(Connection con) throws SQLException {
        StopWatch watch = new StopWatch();
        watch.start();
        String sql = "CREATE TABLE AISInputFiltered AS " +
                "SELECT DISTINCT ON(MMSI,T) * " +
                "FROM AISInput " +
                "WHERE Longitude BETWEEN -16.1 and 32.88 AND Latitude BETWEEN 40.18 AND 84.17; ";

        Statement st = con.createStatement();
        st.execute(sql);
        st.close();
        watch.stop();
        System.out.println("filterData: " + watch.getTime() + "ms");
    }

    /**
     * Load the ship data using MobilityDB JDBC instead of a query to test the driver
     * See Workshop documentation 1.6 Constructing Trajectories
     * @param con - SQL connection
     * @throws SQLException - If load fails
     */
    private static void loadShipData(Connection con) throws SQLException {
        StopWatch watch = new StopWatch();
        watch.start();
        String sql = "SELECT MMSI, T, ST_Transform(Geom, 25832), SOG, COG " +
                "FROM AISInputFiltered " +
                "ORDER BY MMSI, T; ";
        Statement readStatement = con.createStatement();
        ResultSet rs = readStatement.executeQuery(sql);
        boolean first = false;
        int currentMmsi = 0;
        List<TGeomPointInst> pointList = new ArrayList<>();
        List<TFloatInst> sogList = new ArrayList<>();
        List<TFloatInst> cogList = new ArrayList<>();

        while (rs.next()) {
            int mmsi = rs.getInt(1);

            if (!first) {
                currentMmsi = mmsi;
            }

            if (mmsi != currentMmsi) {
                saveShip(con, currentMmsi, pointList, sogList, cogList);
                currentMmsi = mmsi;
                pointList = new ArrayList<>();
                sogList = new ArrayList<>();
                cogList = new ArrayList<>();
            }

            OffsetDateTime time = rs.getObject(2, OffsetDateTime.class);
            PGgeometry pgGeometry = (PGgeometry) rs.getObject(3);
            pointList.add(new TGeomPointInst((Point)pgGeometry.getGeometry(), time));
            float sog = rs.getFloat(4);

            if (!rs.wasNull()) {
                sogList.add(new TFloatInst(sog, time));
            }

            float cog = rs.getFloat(5);

            if (!rs.wasNull()) {
                cogList.add(new TFloatInst(cog, time));
            }

            first = true;
        }

        saveShip(con, currentMmsi, pointList, sogList, cogList);

        readStatement.close();
        watch.stop();
        System.out.println("loadShipData: " + watch.getTime() + "ms");
    }

    private static void saveShip(Connection con, int mmsi,
                                 List<TGeomPointInst> pointList,
                                 List<TFloatInst> sogList,
                                 List<TFloatInst> cogList) throws SQLException {
        TGeomPointSeq pointSeq = new TGeomPointSeq(pointList.toArray(new TGeomPointInst[0]));
        TFloatSeq sogSeq = new TFloatSeq(sogList.toArray(new TFloatInst[0]));
        TFloatSeq cogSeq = new TFloatSeq(cogList.toArray(new TFloatInst[0]));

        PreparedStatement insertStatement = con.prepareStatement(
                "INSERT INTO ships( " +
                        "mmsi, trip, sog, cog) " +
                        "VALUES (?, ?, ?, ?);");
        insertStatement.setInt(1, mmsi);
        insertStatement.setObject(2, new TGeomPoint(pointSeq));
        insertStatement.setObject(3, new TFloat(sogSeq));
        insertStatement.setObject(4, new TFloat(cogSeq));
        insertStatement.execute();
        insertStatement.close();
    }

    /**
     * Set the trajectory using MobilityDB JDBC instead of a query to test the driver
     * See Workshop documentation 1.6 Constructing Trajectories
     * @param con - SQL connection
     * @throws SQLException - If load fails
     */
    private static void setTrajectory(Connection con) throws SQLException {
        StopWatch watch = new StopWatch();
        watch.start();
        String sql = "SELECT MMSI, TRIP FROM SHIPS;";
        Statement readStatement = con.createStatement();
        ResultSet rs = readStatement.executeQuery(sql);

        while (rs.next()) {
            int mmsi = rs.getInt(1);
            TGeomPoint tGeomPoint = (TGeomPoint) rs.getObject(2);
            updateShipTrajectory(con, mmsi, tGeomPoint);
        }

        readStatement.close();
        watch.stop();
        System.out.println("setTrajectory: " + watch.getTime() + "ms");
    }

    private static void updateShipTrajectory(Connection con, int mmsi, TGeomPoint tGeomPoint) throws SQLException {
        PreparedStatement insertStatement = con.prepareStatement(
                "UPDATE ships " +
                        "SET traj=trajectory(?) " +
                        "WHERE mmsi=?;");
        insertStatement.setObject(1, tGeomPoint);
        insertStatement.setInt(2, mmsi);
        insertStatement.execute();
        insertStatement.close();
    }
}
