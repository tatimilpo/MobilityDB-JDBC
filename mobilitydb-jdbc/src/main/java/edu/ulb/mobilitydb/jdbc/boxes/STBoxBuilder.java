package edu.ulb.mobilitydb.jdbc.boxes;

import java.sql.SQLException;
import java.time.OffsetDateTime;

public class STBoxBuilder {
    private Double xmin = null;
    private Double xmax = null;
    private Double ymin = null;
    private Double ymax = null;
    private Double zmin = null;
    private Double zmax = null;
    private OffsetDateTime tmin = null;
    private OffsetDateTime tmax = null;
    private boolean isGeodetic = false;
    private int srid = 0;

    public STBoxBuilder setXYCoordinates(double xmin, double ymin, double xmax, double ymax) {
        this.xmin = xmin;
        this.xmax = xmax;
        this.ymin = ymin;
        this.ymax = ymax;
        return this;
    }

    public STBoxBuilder setXYZCoordinates(double xmin, double ymin, double zmin, double xmax, double ymax, double zmax) {
        this.xmin = xmin;
        this.xmax = xmax;
        this.ymin = ymin;
        this.ymax = ymax;
        this.zmin = zmin;
        this.zmax = zmax;
        return this;
    }

    public STBoxBuilder setTime(OffsetDateTime tmin, OffsetDateTime tmax) {
        this.tmin = tmin;
        this.tmax = tmax;
        return this;
    }

    public STBoxBuilder setSrid(int srid) {
        this.srid = srid;
        return this;
    }

    public STBoxBuilder isGeodetic(boolean isGeodetic) {
        this.isGeodetic = isGeodetic;
        return this;
    }

    public STBox build() throws SQLException{
        if (xmin == null && tmin == null){
            throw new SQLException("Could not parse STBox value, invalid number of arguments.");
        }
        return new STBox(xmin, ymin, zmin, tmin, xmax, ymax, zmax, tmax, srid, isGeodetic);
    }
    
}
