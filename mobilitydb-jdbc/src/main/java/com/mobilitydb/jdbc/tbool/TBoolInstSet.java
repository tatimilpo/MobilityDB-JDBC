package com.mobilitydb.jdbc.tbool;

import com.mobilitydb.jdbc.temporal.TInstantSet;

import java.sql.SQLException;

public class TBoolInstSet extends TInstantSet<Boolean> {
    public TBoolInstSet(String value) throws SQLException {
        super(value, TBoolInst::new, TBool::compareValue);
    }

    public TBoolInstSet(String[] values) throws SQLException {
        super(values, TBoolInst::new, TBool::compareValue);
    }

    public TBoolInstSet(TBoolInst[] values) throws SQLException {
        super(values, TBool::compareValue);
    }
}
