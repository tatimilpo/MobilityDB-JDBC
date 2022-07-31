package com.mobilitydb.jdbc.tbool;

import com.mobilitydb.jdbc.temporal.TSequenceSet;

import java.sql.SQLException;

public class TBoolSeqSet extends TSequenceSet<Boolean> {
    public TBoolSeqSet(String value) throws SQLException {
        super(true, value, TBoolSeq::new, TBool::compareValue);
    }

    public TBoolSeqSet(String[] values) throws SQLException {
        super(true, values, TBoolSeq::new, TBool::compareValue);
    }

    public TBoolSeqSet(TBoolSeq[] values) throws SQLException {
        super(true, values, TBool::compareValue);
    }

    @Override
    protected boolean explicitInterpolation() {
        return false;
    }
}
