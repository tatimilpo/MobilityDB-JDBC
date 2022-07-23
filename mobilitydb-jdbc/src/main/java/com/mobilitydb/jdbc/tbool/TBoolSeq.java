package com.mobilitydb.jdbc.tbool;

import com.mobilitydb.jdbc.temporal.TSequence;

import java.sql.SQLException;

/**
 * By Default Interpolation is stepwise
 */
public class TBoolSeq extends TSequence<Boolean> {
    public TBoolSeq(String value) throws SQLException {
        super(value, TBool::getSingleTemporalValue, TBool::compareValue);
        stepwise = true;
    }

    public TBoolSeq(String[] values) throws SQLException {
        super(true, values, TBool::getSingleTemporalValue, TBool::compareValue);
    }

    public TBoolSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(true, values, lowerInclusive, upperInclusive, TBool::getSingleTemporalValue, TBool::compareValue);
    }

    public TBoolSeq(TBoolInst[] values) throws SQLException {
        super(true, values, TBool::compareValue);
    }

    public TBoolSeq(TBoolInst[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(true, values, lowerInclusive, upperInclusive, TBool::compareValue);
    }

    @Override
    protected boolean explicitInterpolation() {
        return false;
    }
}
