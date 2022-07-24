package com.mobilitydb.jdbc.tfloat;

import com.mobilitydb.jdbc.temporal.TInstantSet;

import java.sql.SQLException;

public class TFloatInstSet extends TInstantSet<Float> {
    public TFloatInstSet(String value) throws SQLException {
        super(value, TFloatInst::new, TFloat::compareValue);
    }

    public TFloatInstSet(String[] values) throws SQLException {
        super(values, TFloatInst::new, TFloat::compareValue);
    }

    public TFloatInstSet(TFloatInst[] values) throws SQLException {
        super(values, TFloat::compareValue);
    }
}
