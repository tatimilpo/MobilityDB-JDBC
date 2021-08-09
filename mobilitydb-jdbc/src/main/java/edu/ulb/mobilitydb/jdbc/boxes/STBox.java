package edu.ulb.mobilitydb.jdbc.boxes;

import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

@TypeName(name = "stbox")
public class STBox  extends DataType {
    private Double xmin = null;
    private Double xmax = null;
    private Double ymin = null;
    private Double ymax = null;
    private Double zmin = null;
    private Double zmax = null;
    private OffsetDateTime tmin;
    private OffsetDateTime tmax;
    private boolean isGeodetic = false;
    private boolean dimT = false;
    private int srid = 0;
    private static final String FORMAT = "yyyy-MM-dd HH:mm:ssX";

    public STBox() {
        super();
    }

    public STBox(final String value) throws SQLException {
        super();
        setValue(value);
    }

    public STBox(Double xmin, Double ymin, Double zmin, OffsetDateTime tmin,
                 Double xmax, Double ymax, Double zmax, OffsetDateTime tmax,
                 int srid, boolean isGeodetic, boolean dimT) {
        super();
        this.xmin = xmin;
        this.xmax = xmax;
        this.ymin = ymin;
        this.ymax = ymax;
        this.zmin = zmin;
        this.zmax = zmax;
        this.tmin = tmin;
        this.tmax = tmax;
        this.srid = srid;
        this.isGeodetic = isGeodetic;
        this.dimT = dimT;
    }

    @Override
    public String getValue() {
        String sridPrefix = null;
        if(srid == 0) {
            sridPrefix = String.format("SRID=%s;", srid);
        }
        if (isGeodetic) {
            if (tmin != null) {
                if (xmin != null) {
                    return String.format("%sGEODSTBOX T((%f, %f, %f, %s), (%f, %f, %f, %s))", sridPrefix,
                            xmin, ymin, zmin, tmin, xmax, ymax, zmax, tmax);
                } else {
                    return String.format("%sGEODSTBOX T((, %s), (, %s))", sridPrefix, tmin, tmax);
                }
            } else {
                return String.format("%sGEODSTBOX((%f, %f, %f), (%f, %f, %f))", sridPrefix,
                        xmin, ymin, zmin, xmax, ymax, zmax);
            }
        } else {
            if(xmin != null && zmin != null && tmin != null) {
                return String.format("%sSTBOX ZT((%f, %f, %f, %s), (%f, %f, %f, %s))", sridPrefix,
                        xmin, ymin, zmin, tmin, xmax, ymax, zmax, tmax);
            } else if (xmin != null && zmin != null && tmin == null) {
                return String.format("%sSTBOX Z((%f, %f, %s), (%f, %f, %s))", sridPrefix,
                        xmin, ymin, zmin, xmax, ymax, zmax);
            } else if (xmin != null && zmin == null && tmin != null) {
                return String.format("%sSTBOX T((%f, %f, %s), (%f, %f, %s))", sridPrefix,
                        xmin, ymin, tmin, xmax, ymax, tmax);
            } else if (xmin != null && zmin == null && tmin == null) {
                return String.format("%sSTBOX ((%f, %f), (%f, %f))", sridPrefix,
                        xmin, ymin, xmax, ymax);
            } else if (xmin == null && zmin == null && tmin != null) {
                return String.format("%sSTBOX T((, %s), (, %s))", sridPrefix, tmin, tmax);
            }
            else {
                return null;
            }
        }
    }


    @Override
    public void setValue(String value) throws SQLException {
        boolean hasZ = false;
        boolean hasT = false;
        if(value.startsWith("SRID")) {
            String[] initialValues = value.split(";");
            srid = Integer.parseInt(initialValues[0].split("=")[1]);
            value = initialValues[1];
        }

        if(value.contains("GEODSTBOX")) {
            isGeodetic = true;
            value = value.replace("GEODSTBOX","");
            hasZ = true;
            hasT = value.contains("T");
        } else if (value.startsWith("STBOX")) {
            value = value.replace("STBOX","");
            hasZ = value.contains("Z");
            hasT = value.contains("T");
        } else {
            throw new SQLException("Could not parse STBox value");
        }
        value = value.replace("Z", "")
                .replace("T", "")
                .replace("(","")
                .replace(")","");
        String[] values = value.split(",");
        DateTimeFormatter format = DateTimeFormatter.ofPattern(FORMAT);

        if (Arrays.stream(values).filter(x -> !x.isBlank()).count() == 2) {
            this.tmin = OffsetDateTime.parse(values[1].trim(), format);
            this.tmax = OffsetDateTime.parse(values[3].trim(), format);
        } else {
            int nonEmpty = (int) Arrays.stream(values).filter(x -> !x.isBlank()).count();
            if ( nonEmpty >= 4) {
                this.xmin = Double.parseDouble(values[0]);
                this.xmax = Double.parseDouble(values[nonEmpty/2]);
                this.ymin = Double.parseDouble(values[1]);
                this.ymax = Double.parseDouble(values[1 + nonEmpty/2]);
            }
            if (hasZ) {
                this.zmin = Double.parseDouble(values[2]);
                this.zmax = Double.parseDouble(values[2 + nonEmpty/2]);
            }
            if (hasT) {
                this.tmin = OffsetDateTime.parse(values[nonEmpty/2 - 1].trim(), format);
                this.tmax = OffsetDateTime.parse(values[(nonEmpty/2 - 1) + nonEmpty/2].trim(), format);
            }
        }

    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        String value = getValue();
        return value != null ? value.hashCode() : 0;
    }

    public double getXmin() {
        return xmin;
    }

    public double getXmax() {
        return xmax;
    }

    public double getYmin() {
        return ymin;
    }

    public double getYmax() {
        return ymax;
    }

    public double getZmin() {
        return zmin;
    }

    public double getZmax() {
        return zmax;
    }

    public OffsetDateTime getTmin() {
        return tmin;
    }

    public OffsetDateTime getTmax() {
        return tmax;
    }

    public boolean isGeodetic() {
        return isGeodetic;
    }

    public int getSrid() {
        return srid;
    }

    public boolean isDimT() {
        return dimT;
    }
}
