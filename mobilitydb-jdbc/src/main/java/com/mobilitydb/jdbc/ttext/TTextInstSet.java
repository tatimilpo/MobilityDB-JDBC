package com.mobilitydb.jdbc.ttext;

import com.mobilitydb.jdbc.temporal.TInstantSet;

import java.sql.SQLException;

public class TTextInstSet extends TInstantSet<String> {
    public TTextInstSet(String value) throws SQLException {
        super(value, TTextInst::new, TText::compareValue);
    }

    public TTextInstSet(String[] values) throws SQLException {
        super(values, TTextInst::new, TText::compareValue);
    }

    public TTextInstSet(TTextInst[] values) throws SQLException {
        super(values, TText::compareValue);
    }
}
