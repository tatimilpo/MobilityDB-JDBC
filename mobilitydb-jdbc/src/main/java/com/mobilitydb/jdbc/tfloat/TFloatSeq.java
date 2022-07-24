package com.mobilitydb.jdbc.tfloat;

import com.mobilitydb.jdbc.temporal.TSequence;

import java.sql.SQLException;

public class TFloatSeq extends TSequence<Float> {
    public TFloatSeq(String value) throws SQLException {
        super(value, TFloatInst::new, TFloat::compareValue);
    }

    public TFloatSeq(String[] values) throws SQLException {
        super(false, values, TFloatInst::new, TFloat::compareValue);
    }

    public TFloatSeq(boolean isStepwise, String[] values) throws SQLException {
        super(isStepwise, values, TFloatInst::new, TFloat::compareValue);
    }

    public TFloatSeq(String[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(false, values, lowerInclusive, upperInclusive, TFloatInst::new, TFloat::compareValue);
    }

    public TFloatSeq(boolean isStepwise, String[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(isStepwise, values, lowerInclusive, upperInclusive, TFloatInst::new, TFloat::compareValue);
    }

    public TFloatSeq(TFloatInst[] values) throws SQLException {
        super(false, values, TFloat::compareValue);
    }

    public TFloatSeq(boolean isStepwise, TFloatInst[] values) throws SQLException {
        super(isStepwise, values, TFloat::compareValue);
    }

    public TFloatSeq(TFloatInst[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(false, values, lowerInclusive, upperInclusive, TFloat::compareValue);
    }

    public TFloatSeq(boolean isStepwise, TFloatInst[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(isStepwise, values, lowerInclusive, upperInclusive, TFloat::compareValue);
    }

}
