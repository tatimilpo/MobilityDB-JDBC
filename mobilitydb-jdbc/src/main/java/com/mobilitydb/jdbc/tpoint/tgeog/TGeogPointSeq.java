package com.mobilitydb.jdbc.tpoint.tgeog;

import com.mobilitydb.jdbc.tpoint.helpers.TPointConstants;
import com.mobilitydb.jdbc.tpoint.TPointSeq;

import java.sql.SQLException;

public class TGeogPointSeq extends TPointSeq {
    public TGeogPointSeq(String value) throws SQLException {
        super(value, TGeogPointInst::new);
    }

    public TGeogPointSeq(String[] values) throws SQLException {
        super(TPointConstants.DEFAULT_SRID, false, values, TGeogPointInst::new);
    }

    public TGeogPointSeq(boolean isStepwise, String[] values) throws SQLException {
        super(TPointConstants.DEFAULT_SRID, isStepwise, values, TGeogPointInst::new);
    }

    public TGeogPointSeq(String[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(TPointConstants.DEFAULT_SRID,
                false,
                values,
                lowerInclusive,
                upperInclusive,
                TGeogPointInst::new);
    }

    public TGeogPointSeq(boolean isStepwise, String[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(TPointConstants.DEFAULT_SRID,
                isStepwise,
                values,
                lowerInclusive,
                upperInclusive,
                TGeogPointInst::new);
    }

    public TGeogPointSeq(TGeogPointInst[] values) throws SQLException {
        super(TPointConstants.DEFAULT_SRID, false, values);
    }

    public TGeogPointSeq(boolean isStepwise, TGeogPointInst[] values) throws SQLException {
        super(TPointConstants.DEFAULT_SRID, isStepwise, values);
    }

    public TGeogPointSeq(TGeogPointInst[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(TPointConstants.DEFAULT_SRID, false, values, lowerInclusive, upperInclusive);
    }

    public TGeogPointSeq(boolean isStepwise, TGeogPointInst[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(TPointConstants.DEFAULT_SRID, isStepwise, values, lowerInclusive, upperInclusive);
    }

    public TGeogPointSeq(int srid, String[] values) throws SQLException {
        super(srid, false, values, TGeogPointInst::new);
    }

    public TGeogPointSeq(int srid, boolean isStepwise, String[] values) throws SQLException {
        super(srid, isStepwise, values, TGeogPointInst::new);
    }

    public TGeogPointSeq(int srid, String[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(srid, false, values, lowerInclusive, upperInclusive, TGeogPointInst::new);
    }

    public TGeogPointSeq(int srid, boolean isStepwise, String[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(srid, isStepwise, values, lowerInclusive, upperInclusive, TGeogPointInst::new);
    }

    public TGeogPointSeq(int srid, TGeogPointInst[] values) throws SQLException {
        super(srid, false, values);
    }

    public TGeogPointSeq(int srid, boolean isStepwise, TGeogPointInst[] values) throws SQLException {
        super(srid, isStepwise, values);
    }

    public TGeogPointSeq(int srid, TGeogPointInst[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(srid, false, values, lowerInclusive, upperInclusive);
    }

    public TGeogPointSeq(int srid, boolean isStepwise, TGeogPointInst[] values,
                         boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(srid, isStepwise, values, lowerInclusive, upperInclusive);
    }
}
