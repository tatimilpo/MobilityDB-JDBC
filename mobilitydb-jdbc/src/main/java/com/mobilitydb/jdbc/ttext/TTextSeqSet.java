package com.mobilitydb.jdbc.ttext;

import com.mobilitydb.jdbc.temporal.TSequenceSet;

import java.sql.SQLException;

public class TTextSeqSet extends TSequenceSet<String> {
    public TTextSeqSet(String value) throws SQLException {
        super(true, value, TTextSeq::new, TText::compareValue);
    }

    public TTextSeqSet(String[] values) throws SQLException {
        super(true, values, TTextSeq::new, TText::compareValue);
    }

    public TTextSeqSet(TTextSeq[] values) throws SQLException {
        super(true, values, TText::compareValue);
    }

    @Override
    protected boolean explicitInterpolation() {
        return false;
    }
}
