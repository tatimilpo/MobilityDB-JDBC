package com.mobilitydb.jdbc.temporal;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.StringJoiner;

public abstract class TInstantSet<V extends Serializable> extends TemporalInstants<V> {
    protected TInstantSet(String value,
                          GetSingleTemporalValueFunction<V> getSingleTemporalValue,
                          CompareValue<V> compareValue) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT_SET, compareValue);
        parseValue(value, getSingleTemporalValue);
        validate();
    }

    protected TInstantSet(String[] values,
                          GetSingleTemporalValueFunction<V> getSingleTemporalValue,
                          CompareValue<V> compareValue) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT_SET, compareValue);
        for (String val : values) {
            temporalValues.add(getSingleTemporalValue.run(val.trim()));
        }
        validate();
    }

    protected TInstantSet(TInstant<V>[] values, CompareValue<V> compareValue) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT_SET, compareValue);
        for (TInstant<V> val : values) {
            temporalValues.add(val.getTemporalValue());
        }
        validate();
    }

    @Override
    protected void validateTemporalDataType() throws SQLException {
        // TODO: Implement
    }

    @Override
    public String buildValue() {
        StringJoiner sj = new StringJoiner(", ");
        for (TemporalValue<V> temp : temporalValues) {
            sj.add(temp.toString());
        }
        return String.format("{%s}", sj.toString());
    }

    private void parseValue(String value, GetSingleTemporalValueFunction<V> getSingleTemporalValue) throws SQLException {
        String newValue = preprocessValue(value);
        newValue = newValue.replace("{", "").replace("}", "");
        String[] values = newValue.split(",", -1);
        for (String val : values) {
            temporalValues.add(getSingleTemporalValue.run(val.trim()));
        }
    }
}
