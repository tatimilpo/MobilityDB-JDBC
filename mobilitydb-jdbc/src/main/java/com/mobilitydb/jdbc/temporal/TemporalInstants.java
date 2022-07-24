package com.mobilitydb.jdbc.temporal;

import com.mobilitydb.jdbc.temporal.delegates.CompareValueFunction;

import java.io.Serializable;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

public abstract class TemporalInstants<V extends Serializable> extends Temporal<V> {
    protected final ArrayList<TInstant<V>> instants = new ArrayList<>();
    private final CompareValueFunction<V> compareValueFunction;

    protected TemporalInstants(TemporalType temporalType, CompareValueFunction<V> compareValueFunction) {
        super(temporalType);
        this.compareValueFunction = compareValueFunction;
    }

    @Override
    public List<V> getValues() {
        List<V> values = new ArrayList<>();
        for (TInstant<V> temp : instants) {
            values.add(temp.getValue());
        }
        return values;
    }

    @Override
    public V startValue() {
        if (instants.isEmpty()) {
            return null;
        }

        return instants.get(0).getValue();
    }

    @Override
    public V endValue() {
        if (instants.isEmpty()) {
            return null;
        }

        return instants.get(instants.size() - 1).getValue();
    }

    @Override
    public V minValue() {
        if (instants.isEmpty()) {
            return null;
        }

        V min = instants.get(0).getValue();

        for (int i = 1; i < instants.size(); i++) {
            V value = instants.get(i).getValue();

            if (compareValueFunction.run(value, min) < 0) {
                min = value;
            }
        }

        return min;
    }

    @Override
    public V maxValue() {
        if (instants.isEmpty()) {
            return null;
        }

        V max = instants.get(0).getValue();

        for (int i = 1; i < instants.size(); i++) {
            V value = instants.get(i).getValue();

            if (compareValueFunction.run(value, max) > 0) {
                max = value;
            }
        }

        return max;
    }

    @Override
    public V valueAtTimestamp(OffsetDateTime timestamp) {
        for (TInstant<V> temp : instants) {
            if (timestamp.isEqual(temp.getTimestamp())) {
                return temp.getValue();
            }
        }
        return null;
    }

    @Override
    public OffsetDateTime startTimestamp() {
        return instants.get(0).getTimestamp();
    }

    @Override
    public OffsetDateTime endTimestamp() {
        return instants.get(instants.size() - 1).getTimestamp();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() == obj.getClass()) {
            TemporalInstants<?> otherTemporal = (TemporalInstants<?>) obj;
            if (this.instants.size() != otherTemporal.instants.size()) {
                return false;
            }
            for (int i = 0; i < this.instants.size(); i++) {
                TInstant<V> thisVal = this.instants.get(i);
                TInstant<?> otherVal = otherTemporal.instants.get(i);
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