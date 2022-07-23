package com.mobilitydb.jdbc.tbool;

import com.mobilitydb.jdbc.temporal.TSequenceSet;

import java.sql.SQLException;

public class TBoolSeqSet extends TSequenceSet<Boolean> {
    public TBoolSeqSet(String value) throws SQLException {
        super(value, TBool::getSingleTemporalValue, TBool::compareValue);
        stepwise = true;
    }

    public TBoolSeqSet(String[] values) throws SQLException {
        super(true, values, TBool::getSingleTemporalValue, TBool::compareValue);
    }

    public TBoolSeqSet(TBoolSeq[] values) throws SQLException {
        super(true, values, TBool::getSingleTemporalValue, TBool::compareValue);
    }

    @Override
    protected boolean explicitInterpolation() {
        return false;
    }
}
