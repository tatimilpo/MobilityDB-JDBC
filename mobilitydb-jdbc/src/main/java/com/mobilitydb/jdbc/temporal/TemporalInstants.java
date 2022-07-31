package com.mobilitydb.jdbc.temporal;

import com.mobilitydb.jdbc.temporal.delegates.CompareValueFunction;

import java.io.Serializable;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

public abstract class TemporalInstants<V extends Serializable> extends Temporal<V> {
    protected final ArrayList<TInstant<V>> instantList = new ArrayList<>();
    private final CompareValueFunction<V> compareValueFunction;

    protected TemporalInstants(TemporalType temporalType, CompareValueFunction<V> compareValueFunction) {
        super(temporalType);
        this.compareValueFunction = compareValueFunction;
    }

    @Override
    public List<V> getValues() {
        List<V> values = new ArrayList<>();
        for (TInstant<V> temp : instantList) {
            values.add(temp.getValue());
        }
        return values;
    }

    @Override
    public V startValue() {
        if (instantList.isEmpty()) {
            return null;
        }

        return instantList.get(0).getValue();
    }

    @Override
    public V endValue() {
        if (instantList.isEmpty()) {
            return null;
        }

        return instantList.get(instantList.size() - 1).getValue();
    }

    @Override
    public V minValue() {
        if (instantList.isEmpty()) {
            return null;
        }

        V min = instantList.get(0).getValue();

        for (int i = 1; i < instantList.size(); i++) {
            V value = instantList.get(i).getValue();

            if (compareValueFunction.run(value, min) < 0) {
                min = value;
            }
        }

        return min;
    }

    @Override
    public V maxValue() {
        if (instantList.isEmpty()) {
            return null;
        }

        V max = instantList.get(0).getValue();

        for (int i = 1; i < instantList.size(); i++) {
            V value = instantList.get(i).getValue();

            if (compareValueFunction.run(value, max) > 0) {
                max = value;
            }
        }

        return max;
    }

    @Override
    public V valueAtTimestamp(OffsetDateTime timestamp) {
        for (TInstant<V> temp : instantList) {
            if (timestamp.isEqual(temp.getTimestamp())) {
                return temp.getValue();
            }
        }
        return null;
    }

    @Override
    public int numTimestamps() {
        return instantList.size();
    }

    @Override
    public OffsetDateTime[] timestamps() {
        OffsetDateTime[] result = new OffsetDateTime[instantList.size()];

        for (int i = 0; i < instantList.size(); i++) {
            result[i] = instantList.get(i).getTimestamp();
        }

        return result;
    }

    @Override
    public OffsetDateTime timestampN(int n) throws SQLException {
        if (n >= 0 && n < instantList.size()) {
            return instantList.get(n).getTimestamp();
        }

        throw new SQLException("There is no timestamp at this index.");
    }

    @Override
    public OffsetDateTime startTimestamp() {
        return instantList.get(0).getTimestamp();
    }

    @Override
    public OffsetDateTime endTimestamp() {
        return instantList.get(instantList.size() - 1).getTimestamp();
    }

    @Override
    public int numInstants() {
        return instantList.size();
    }

    @Override
    public TInstant<V> startInstant() {
        return instantList.get(0);
    }

    @Override
    public TInstant<V> endInstant() {
        return instantList.get(instantList.size() - 1);
    }

    @Override
    public TInstant<V> instantN(int n) throws SQLException {
        if (n >= 0 && n < instantList.size()) {
            return instantList.get(n);
        }

        throw new SQLException("There is no instant at this index.");
    }

    @Override
    public List<TInstant<V>> instants() {
        return new ArrayList<>(instantList);
    }

    @Override
    public void shift(Duration duration) {
        for (TInstant<V> instant : instantList) {
            instant.shift(duration);
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() == obj.getClass()) {
            TemporalInstants<?> otherTemporal = (TemporalInstants<?>) obj;
            if (this.instantList.size() != otherTemporal.instantList.size()) {
                return false;
            }
            for (int i = 0; i < this.instantList.size(); i++) {
                TInstant<V> thisVal = this.instantList.get(i);
                TInstant<?> otherVal = otherTemporal.instantList.get(i);
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

    protected void validateInstantList(String type) throws SQLException {
        if (instantList.isEmpty()) {
            throw new SQLException(String.format("%s must be composed of at least one instant.", type));
        }

        for (int i = 0; i < instantList.size(); i++) {
            TInstant<V> x = instantList.get(i);
            validateInstant(x);

            if (i + 1 < instantList.size()) {
                TInstant<V>  y = instantList.get(i + 1);
                validateInstant(y);

                if (x.getTimestamp().isAfter(y.getTimestamp()) || x.getTimestamp().isEqual(y.getTimestamp())) {
                    throw new SQLException(String.format("The timestamps of a %s must be increasing.", type));
                }
            }
        }
    }

    private void validateInstant(TInstant<V> instant) throws SQLException {
        if (instant == null) {
            throw new SQLException("All instants should have a value.");
        }
    }
}
