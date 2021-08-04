package edu.ulb.mobilitydb.jdbc.boxes;

import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

@TypeName(name = "stbox")
public class STBox  extends DataType {
    private float xmin = 0.0f;
    private float xmax = 0.0f;
    private float ymin = 0.0f;
    private float ymax = 0.0f;
    private float zmin = 0.0f;
    private float zmax = 0.0f;
    private OffsetDateTime tmin;
    private OffsetDateTime tmax;
    private boolean isGeodetic = false;
    private int srid;
    private static final String FORMAT = "yyyy-MM-dd HH:mm:ssX";

    public STBox() {
        super();
    }



    public STBox(final String value) throws SQLException {
        super();
        setValue(value);
    }

    @Override
    public String getValue() {
        return null;
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
                this.xmin = Float.parseFloat(values[0]);
                this.xmax = Float.parseFloat(values[nonEmpty/2]);
                this.ymin = Float.parseFloat(values[1]);
                this.ymax = Float.parseFloat(values[1 + nonEmpty/2]);
            }
            if (hasZ) {
                this.zmin = Float.parseFloat(values[2]);
                this.zmax = Float.parseFloat(values[2 + nonEmpty/2]);
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

    public float getXmin() {
        return xmin;
    }

    public float getXmax() {
        return xmax;
    }

    public float getYmin() {
        return ymin;
    }

    public float getYmax() {
        return ymax;
    }

    public float getZmin() {
        return zmin;
    }

    public float getZmax() {
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
}
