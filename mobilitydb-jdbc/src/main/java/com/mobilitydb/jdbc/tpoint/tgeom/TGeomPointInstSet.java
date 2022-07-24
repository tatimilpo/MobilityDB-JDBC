package com.mobilitydb.jdbc.tpoint.tgeom;

import com.mobilitydb.jdbc.tpoint.helpers.TPointConstants;
import com.mobilitydb.jdbc.tpoint.TPointInstSet;

import java.sql.SQLException;

public class TGeomPointInstSet extends TPointInstSet {
    public TGeomPointInstSet(String value) throws SQLException {
        super(value, TGeomPointInst::new);
    }

    public TGeomPointInstSet(String[] values) throws SQLException {
        super(TPointConstants.EMPTY_SRID, values, TGeomPointInst::new);
    }

    public TGeomPointInstSet(int srid, String[] values) throws SQLException {
        super(srid, values, TGeomPointInst::new);
    }

    public TGeomPointInstSet(TGeomPointInst[] values) throws SQLException {
        super(TPointConstants.EMPTY_SRID, values);
    }

    public TGeomPointInstSet(int srid, TGeomPointInst[] values) throws SQLException {
        super(srid, values);
    }
}
