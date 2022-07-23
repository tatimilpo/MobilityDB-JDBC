package com.mobilitydb.jdbc.temporal;

import java.io.Serializable;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;

public abstract class TInstantSet<V extends Serializable> extends Temporal<V> {
    protected final ArrayList<TemporalValue<V>> temporalValues = new ArrayList<>();

    protected TInstantSet(String value, GetSingleTemporalValueFunction<V> getSingleTemporalValue) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT_SET);
        parseValue(value, getSingleTemporalValue);
        validate();
    }

    protected TInstantSet(String[] values, GetSingleTemporalValueFunction<V> getSingleTemporalValue) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT_SET);
        for (String val : values) {
            temporalValues.add(getSingleTemporalValue.run(val.trim()));
        }
        validate();
    }

    protected TInstantSet(TInstant<V>[] values) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT_SET);
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

    @Override
    public List<V> getValues() {
        List<V> values = new ArrayList<>();
        for (TemporalValue<V> temp : temporalValues) {
            values.add(temp.getValue());
        }
        return values;
    }

    @Override
    public V startValue() {
        return temporalValues.get(0).getValue();
    }

    @Override
    public V endValue() {
        return temporalValues.get(temporalValues.size()-1).getValue();
    }

    @Override
    public V minValue() {
        V min = temporalValues.get(0).getValue();
        for (TemporalValue<V> value : temporalValues) {
            if (value.getValue().compareTo(min) < 0) {
                min = value.getValue();
            }
        }
        return min;
    }

    @Override
    public V maxValue() {
        V max = temporalValues.get(0).getValue();
        for (TemporalValue<V> value : temporalValues) {
            if (value.getValue().compareTo(max) > 0) {
                max = value.getValue();
            }
        }
        return max;
    }

    @Override
    public V valueAtTimestamp(OffsetDateTime timestamp) {
        for (TemporalValue<V> temp : temporalValues) {
            if (timestamp.isEqual(temp.getTime())) {
                return temp.getValue();
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() == obj.getClass()) {
            TInstantSet<?> otherTemporal = (TInstantSet<?>) obj;
            if (this.temporalValues.size() != otherTemporal.temporalValues.size()) {
                return false;
            }
            for (int i = 0; i < this.temporalValues.size(); i++) {
                TemporalValue<V> thisVal = this.temporalValues.get(i);
                TemporalValue<?> otherVal = otherTemporal.temporalValues.get(i);
                if (!thisVal.equals(otherVal)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public int hashCode() {
        String value = toString();
        return value != null ? value.hashCode() : 0;
    }
}
