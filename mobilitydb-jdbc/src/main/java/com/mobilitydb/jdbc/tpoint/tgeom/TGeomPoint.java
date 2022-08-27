package com.mobilitydb.jdbc.tpoint.tgeom;

import com.mobilitydb.jdbc.core.TypeName;
import com.mobilitydb.jdbc.temporal.Temporal;
import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.tpoint.TPoint;
import org.postgis.Point;


import java.sql.SQLException;

/**
 * Class that represents the MobilityDB type TGeomPoint
 */
@TypeName(name = "tgeompoint")
public class TGeomPoint extends TPoint {
    public TGeomPoint() { super(); }

    public TGeomPoint(String value) throws SQLException {
        super(value);
    }

    public TGeomPoint(Temporal<Point> temporal) {
        super(temporal);
    }

    @Override
    public void setValue(String value) throws SQLException {
        TemporalType temporalType = getTemporalType(value);
        switch (temporalType) {
            case TEMPORAL_INSTANT:
                temporal = new TGeomPointInst(value);
                break;
            case TEMPORAL_INSTANT_SET:
                temporal = new TGeomPointInstSet(value);
                break;
            case TEMPORAL_SEQUENCE:
                temporal = new TGeomPointSeq(value);
                break;
            case TEMPORAL_SEQUENCE_SET:
                temporal = new TGeomPointSeqSet(value);
                break;
        }
    }
}
