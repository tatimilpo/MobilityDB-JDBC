package com.mobilitydb.jdbc.tint;

import com.mobilitydb.jdbc.temporal.TInstantSet;

import java.sql.SQLException;

public class TIntInstSet extends TInstantSet<Integer> {

    //String of "{10@2019-09-08, 20@2019-09-09, 20@2019-09-10}"
    public TIntInstSet(String value) throws SQLException {
        super(value, TIntInst::new, TInt::compareValue);
    }

    //Array of strings "10@2019-09-08"
    public TIntInstSet(String[] values) throws SQLException {
        super(values, TIntInst::new, TInt::compareValue);
    }

    //Array of TIntInst
    public TIntInstSet(TIntInst[] values) throws SQLException {
        super(values, TInt::compareValue);
    }
}
