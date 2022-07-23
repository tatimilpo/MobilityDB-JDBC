package com.mobilitydb.jdbc.temporal;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

public abstract class TemporalInstants<V extends Serializable> extends Temporal<V> {
    protected final ArrayList<TemporalValue<V>> temporalValues = new ArrayList<>();
    private final CompareValue<V> compareValue;

    protected TemporalInstants(TemporalType temporalType, CompareValue<V> compareValue) {
        super(temporalType);
        this.compareValue = compareValue;
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
            if (compareValue.run(value.getValue(), min) < 0) {
                min = value.getValue();
            }
        }
        return min;
    }

    @Override
    public V maxValue() {
        V max = temporalValues.get(0).getValue();
        for (TemporalValue<V> value : temporalValues) {
            if (compareValue.run(value.getValue(), max) > 0) {
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
            TemporalInstants<?> otherTemporal = (TemporalInstants<?>) obj;
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
