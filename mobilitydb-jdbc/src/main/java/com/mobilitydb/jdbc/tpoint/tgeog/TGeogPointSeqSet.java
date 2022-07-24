package com.mobilitydb.jdbc.tpoint.tgeog;

import com.mobilitydb.jdbc.tpoint.TPointSeqSet;
import com.mobilitydb.jdbc.tpoint.helpers.TPointConstants;

import java.sql.SQLException;

public class TGeogPointSeqSet extends TPointSeqSet {
    public TGeogPointSeqSet(String value) throws SQLException {
        super(value, TGeogPointSeq::new);
    }

    public TGeogPointSeqSet(String[] values) throws SQLException {
        super(TPointConstants.DEFAULT_SRID, false, values, TGeogPointSeq::new);
    }

    public TGeogPointSeqSet(boolean stepwise, String[] values) throws SQLException {
        super(TPointConstants.DEFAULT_SRID, stepwise, values, TGeogPointSeq::new);
    }

    public TGeogPointSeqSet(TGeogPointSeq[] values) throws SQLException {
        super(TPointConstants.DEFAULT_SRID, false, values);
    }

    public TGeogPointSeqSet(boolean stepwise, TGeogPointSeq[] values) throws SQLException {
        super(TPointConstants.DEFAULT_SRID, stepwise, values);
    }

    public TGeogPointSeqSet(int srid, String[] values) throws SQLException {
        super(srid, false, values, TGeogPointSeq::new);
    }

    public TGeogPointSeqSet(int srid, boolean stepwise, String[] values) throws SQLException {
        super(srid, stepwise, values, TGeogPointSeq::new);
    }

    public TGeogPointSeqSet(int srid, TGeogPointSeq[] values) throws SQLException {
        super(srid, false, values);
    }

    public TGeogPointSeqSet(int srid, boolean stepwise, TGeogPointSeq[] values) throws SQLException {
        super(srid, stepwise, values);
    }
}
