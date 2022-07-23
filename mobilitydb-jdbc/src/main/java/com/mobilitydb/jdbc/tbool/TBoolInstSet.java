package com.mobilitydb.jdbc.tbool;

import com.mobilitydb.jdbc.temporal.TInstantSet;

import java.sql.SQLException;

public class TBoolInstSet extends TInstantSet<Boolean> {
    public TBoolInstSet(String value) throws SQLException {
        super(value, TBool::getSingleTemporalValue, TBool::compareValue);
    }

    public TBoolInstSet(String[] values) throws SQLException {
        super(values, TBool::getSingleTemporalValue, TBool::compareValue);
    }

    public TBoolInstSet(TBoolInst[] values) throws SQLException {
        super(values, TBool::compareValue);
    }
}
