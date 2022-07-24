package com.mobilitydb.jdbc.tpoint.tgeom;

import com.mobilitydb.jdbc.tpoint.TPointSeqSet;
import com.mobilitydb.jdbc.tpoint.helpers.TPointConstants;

import java.sql.SQLException;

public class TGeomPointSeqSet extends TPointSeqSet {
    public TGeomPointSeqSet(String value) throws SQLException {
        super(value, TGeomPointSeq::new);
    }

    public TGeomPointSeqSet(String[] values) throws SQLException {
        super(TPointConstants.EMPTY_SRID, false, values, TGeomPointSeq::new);
    }

    public TGeomPointSeqSet(boolean stepwise, String[] values) throws SQLException {
        super(TPointConstants.EMPTY_SRID, stepwise, values, TGeomPointSeq::new);
    }

    public TGeomPointSeqSet(TGeomPointSeq[] values) throws SQLException {
        super(TPointConstants.EMPTY_SRID, false, values);
    }

    public TGeomPointSeqSet(boolean stepwise, TGeomPointSeq[] values) throws SQLException {
        super(TPointConstants.EMPTY_SRID, stepwise, values);
    }

    public TGeomPointSeqSet(int srid, String[] values) throws SQLException {
        super(srid, false, values, TGeomPointSeq::new);
    }

    public TGeomPointSeqSet(int srid, boolean stepwise, String[] values) throws SQLException {
        super(srid, stepwise, values, TGeomPointSeq::new);
    }

    public TGeomPointSeqSet(int srid, TGeomPointSeq[] values) throws SQLException {
        super(srid, false, values);
    }

    public TGeomPointSeqSet(int srid, boolean stepwise, TGeomPointSeq[] values) throws SQLException {
        super(srid, stepwise, values);
    }
}
