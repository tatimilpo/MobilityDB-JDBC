package com.mobilitydb.jdbc.tbool;

import com.mobilitydb.jdbc.temporal.TemporalDataType;
import com.mobilitydb.jdbc.temporal.TemporalType;
import com.mobilitydb.jdbc.temporal.TemporalValue;
import com.mobilitydb.jdbc.core.DateTimeFormatHelper;
import com.mobilitydb.jdbc.core.TypeName;
import com.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;

@TypeName(name = "tbool")
public class TBool extends TemporalDataType<Boolean> {
    public TBool() {
        super();
    }

    public TBool(final String value) throws SQLException {
        super(value);
    }

    public TBool(Temporal<Boolean> temporal) {
        super();
        this.temporal = temporal;
    }

    @Override
    public void setValue(final String value) throws SQLException {
        TemporalType temporalType = TemporalType.getTemporalType(value, this.getClass().getSimpleName());
        switch (temporalType) {
            case TEMPORAL_INSTANT:
                temporal = new TBoolInst(value);
                break;
            case TEMPORAL_INSTANT_SET:
                temporal = new TBoolInstSet(value);
                break;
            case TEMPORAL_SEQUENCE:
                temporal = new TBoolSeq(value);
                break;
            case TEMPORAL_SEQUENCE_SET:
                temporal = new TBoolSeqSet(value);
                break;
        }
    }

    public static TemporalValue<Boolean> getSingleTemporalValue(String value) {
        boolean b;
        String[] values = value.trim().split("@");
        if(values[0].length() == 1) {
            b = values[0].equals("t");
        } else {
            b = Boolean.parseBoolean(values[0]);
        }
        return new TemporalValue<>(b, DateTimeFormatHelper.getDateTimeFormat(values[1]));
    }

    public static int compareValue(Boolean first, Boolean second) {
        return first.compareTo(second);
    }
}