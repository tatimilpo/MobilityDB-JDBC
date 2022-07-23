package com.mobilitydb.jdbc.tint;

import com.mobilitydb.jdbc.temporal.TemporalDataType;
import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.temporal.TemporalValue;
import com.mobilitydb.jdbc.core.DateTimeFormatHelper;
import com.mobilitydb.jdbc.core.TypeName;
import com.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;

@TypeName(name = "tint")
public class TInt extends TemporalDataType<Integer> {
    public TInt() {
        super();
    }

    public TInt(final String value) throws SQLException {
        super(value);
    }

    public TInt(Temporal<Integer> temporal) {
        super();
        this.temporal = temporal;
    }

    @Override
    public void setValue(final String value) throws SQLException {
        TemporalType temporalType = TemporalType.getTemporalType(value, this.getClass().getSimpleName());
        switch (temporalType) {
            case TEMPORAL_INSTANT:
                temporal = new TIntInst(value);
                break;
            case TEMPORAL_INSTANT_SET:
                temporal = new TIntInstSet(value);
                break;
            case TEMPORAL_SEQUENCE:
                temporal = new TIntSeq(value);
                break;
            case TEMPORAL_SEQUENCE_SET:
                temporal = new TIntSeqSet(value);
                break;
        }
    }

    public static TemporalValue<Integer> getSingleTemporalValue(String value) {
        String[] values = value.trim().split("@");
        return new TemporalValue<>(Integer.parseInt(values[0]), DateTimeFormatHelper.getDateTimeFormat(values[1]));
    }

    public static int compareValue(Integer first, Integer second) {
        return first.compareTo(second);
    }
}
