package edu.ulb.mobilitydb.jdbc.boxes;

import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Objects;

@TypeName(name = "stbox")
public class STBox  extends DataType {
    private Double xmin;
    private Double xmax;
    private Double ymin;
    private Double ymax;
    private Double zmin;
    private Double zmax;
    private OffsetDateTime tmin;
    private OffsetDateTime tmax;
    private boolean isGeodetic;

    private int srid;
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
                 int srid, boolean isGeodetic) throws SQLException {
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
        validate();
    }

    @Override
    public String getValue() {
        String sridPrefix = "";
        if(srid != 0) {
            sridPrefix = String.format("SRID=%s;", srid);
        }
        if (isGeodetic) {
            return getGeodeticValue(sridPrefix);
        } else {
            return getNonGeodeticValue(sridPrefix);
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
        int nonEmpty = (int) Arrays.stream(values).filter(x -> !x.isBlank()).count();
        if (nonEmpty == 2) {
            String[] removedNull = Arrays.stream(values)
                    .filter(x -> !x.isBlank())
                    .toArray(String[]::new);
            this.tmin = OffsetDateTime.parse(removedNull[0].trim(), format);
            this.tmax = OffsetDateTime.parse(removedNull[1].trim(), format);
        } else {
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
        validate();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof STBox) {
            STBox fobj = (STBox) obj;
            boolean tIsEqual;

            if (tmin != null && tmax != null) {
                tIsEqual = tmax.isEqual(fobj.tmax) && tmin.isEqual(fobj.tmin);
            } else {
                tIsEqual = tmax == fobj.tmax && tmin == fobj.tmin;
            }

            boolean xIsEqual = Objects.equals(xmin, fobj.xmin) && Objects.equals(xmax, fobj.xmax);
            boolean yIsEqual = Objects.equals(ymin, fobj.ymin) && Objects.equals(ymax, fobj.ymax);
            boolean zIsEqual = Objects.equals(zmin, fobj.zmin) && Objects.equals(zmax, fobj.zmax);

            return xIsEqual && yIsEqual && zIsEqual && tIsEqual && isGeodetic == fobj.isGeodetic();
        }
        return false;
    }

    @Override
    public int hashCode() {
        String value = getValue();
        return value != null ? value.hashCode() : 0;
    }

    public Double getXmin() {
        return xmin;
    }

    public Double getXmax() {
        return xmax;
    }

    public Double getYmin() {
        return ymin;
    }

    public Double getYmax() {
        return ymax;
    }

    public Double getZmin() {
        return zmin;
    }

    public Double getZmax() {
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

    private void validate() throws SQLException {
        if (tmin == null ^ tmax == null) {
            throw new SQLException("Both tmin and tmax should have a value.");
        }

        if((xmin == null ^ xmax == null) ^ (ymin == null ^ ymax == null)) {
            throw new SQLException("Both x and y coordinates should have a value.");
        }

        if(zmin == null ^ zmax == null) {
            throw new SQLException("Both zmax and zmin should have a value.");
        }

        if (xmin == null && tmin == null){
            throw new SQLException("Could not parse STBox value, invalid number of arguments.");
        }
    }

    private String getGeodeticValue(String sridPrefix) {
        if (tmin != null) {
            if (xmin != null) {
                return String.format("%sGEODSTBOX T((%f, %f, %f, %s), (%f, %f, %f, %s))", sridPrefix,
                        xmin, ymin, zmin, tmin, xmax, ymax, zmax, tmax);
            }
            return String.format("%sGEODSTBOX T((, %s), (, %s))", sridPrefix, tmin, tmax);
        }
        return String.format("%sGEODSTBOX((%f, %f, %f), (%f, %f, %f))", sridPrefix,
                xmin, ymin, zmin, xmax, ymax, zmax);
    }

    private String getNonGeodeticValue(String sridPrefix) {
        if (xmin == null) {
            if (tmin != null) {
                return String.format("%sSTBOX T((, %s), (, %s))", sridPrefix, tmin, tmax);
            }
        } else if (zmin == null) {
            if (tmin == null) {
                return String.format("%sSTBOX ((%f, %f), (%f, %f))", sridPrefix,
                        xmin, ymin, xmax, ymax);
            }
            return String.format("%sSTBOX T((%f, %f, %s), (%f, %f, %s))", sridPrefix,
                    xmin, ymin, tmin, xmax, ymax, tmax);
        } else {
            if (tmin == null) {
                return String.format("%sSTBOX Z((%f, %f, %s), (%f, %f, %s))", sridPrefix,
                        xmin, ymin, zmin, xmax, ymax, zmax);
            }
            return String.format("%sSTBOX ZT((%f, %f, %f, %s), (%f, %f, %f, %s))", sridPrefix,
                    xmin, ymin, zmin, tmin, xmax, ymax, zmax, tmax);
        }
        return null;
    }
}
