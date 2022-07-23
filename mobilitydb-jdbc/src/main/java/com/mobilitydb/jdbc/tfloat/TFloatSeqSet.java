package com.mobilitydb.jdbc.tfloat;

import com.mobilitydb.jdbc.temporal.TSequenceSet;


import java.sql.SQLException;

public class TFloatSeqSet extends TSequenceSet<Float> {
    public TFloatSeqSet(String value) throws SQLException {
        super(value, TFloat::getSingleTemporalValue, TFloat::compareValue);
    }

    public TFloatSeqSet(String[] values) throws SQLException {
        super(false, values, TFloat::getSingleTemporalValue, TFloat::compareValue);
    }

    public TFloatSeqSet(boolean stepwise, String[] values) throws SQLException {
        super(stepwise, values, TFloat::getSingleTemporalValue, TFloat::compareValue);
    }

    public TFloatSeqSet(TFloatSeq[] values) throws SQLException {
        super(false, values, TFloat::getSingleTemporalValue, TFloat::compareValue);
    }

    public TFloatSeqSet(boolean stepwise, TFloatSeq[] values) throws SQLException {
        super(stepwise, values, TFloat::getSingleTemporalValue, TFloat::compareValue);
    }
}
