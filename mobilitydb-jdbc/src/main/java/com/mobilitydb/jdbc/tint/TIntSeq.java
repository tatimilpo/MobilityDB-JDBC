package com.mobilitydb.jdbc.tint;

import com.mobilitydb.jdbc.temporal.TSequence;

import java.sql.SQLException;

/**
 * By Default Interpolation is stepwise
 */
public class TIntSeq extends TSequence<Integer> {
    public TIntSeq(String value) throws SQLException {
        super(true, value, TIntInst::new, TInt::compareValue);
    }

    public TIntSeq(String[] values) throws SQLException {
        super(true, values, TIntInst::new, TInt::compareValue);
    }

    public TIntSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(true, values, lowerInclusive, upperInclusive, TIntInst::new, TInt::compareValue);
    }

    public TIntSeq(TIntInst[] values) throws SQLException {
        super(true, values, TInt::compareValue);
    }

    public TIntSeq(TIntInst[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(true, values, lowerInclusive, upperInclusive, TInt::compareValue);
    }

    @Override
    protected boolean explicitInterpolation() {
        return false;
    }
}
