package com.mobilitydb.jdbc.tint;

import com.mobilitydb.jdbc.temporal.TSequenceSet;

import java.sql.SQLException;

public class TIntSeqSet extends TSequenceSet<Integer> {
    public TIntSeqSet(String value) throws SQLException {
        super(true, value, TIntSeq::new, TInt::compareValue);
    }

    public TIntSeqSet(String[] values) throws SQLException {
        super(true, values, TIntSeq::new, TInt::compareValue);
    }

    public TIntSeqSet(TIntSeq[] values) throws SQLException {
        super(true, values, TInt::compareValue);
    }

    @Override
    protected boolean explicitInterpolation() {
        return false;
    }
}
