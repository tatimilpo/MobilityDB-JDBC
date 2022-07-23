package com.mobilitydb.jdbc.temporal;

import java.io.Serializable;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;

public abstract class TInstant<V extends Serializable> extends Temporal<V> {
    private final TemporalValue<V> temporalValue;

    protected TInstant(String value, GetSingleTemporalValueFunction<V> getSingleTemporalValue) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT);
        temporalValue = getSingleTemporalValue.run(preprocessValue(value));
        validate();
    }

    protected TInstant(V value, OffsetDateTime time) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT);
        temporalValue = buildTemporalValue(value, time);
        validate();
    }

    @Override
    protected void validateTemporalDataType() throws SQLException {
        if (temporalValue == null) {
            throw new SQLException("Temporal value cannot be null.");
        }

        if (temporalValue.getTime() == null) {
            throw new SQLException("Timestamp cannot be null.");
        }
    }

    @Override
    public String buildValue() {
        return temporalValue.toString();
    }

    public TemporalValue<V> getTemporalValue() {
        return temporalValue;
    }

    public V getValue() {
        return temporalValue.getValue();
    }

    @Override
    public List<V> getValues() {
        List<V> values = new ArrayList<>();
        values.add(temporalValue.getValue());
        return values;
    }

    @Override
    public V startValue() {
        return temporalValue.getValue();
    }

    @Override
    public V endValue() {
        return temporalValue.getValue();
    }

    @Override
    public V minValue() {
        return temporalValue.getValue();
    }

    @Override
    public V maxValue() {
        return temporalValue.getValue();
    }

    @Override
    public V valueAtTimestamp(OffsetDateTime timestamp) {
        if (timestamp.isEqual(temporalValue.getTime())) {
            return temporalValue.getValue();
        }
        return null;
    }

    public OffsetDateTime getTimestamp() {
        return temporalValue.getTime();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() == obj.getClass()) {
            TInstant<?> otherTemporal = (TInstant<?>) obj;
            return this.temporalValue.equals(otherTemporal.temporalValue);
        }
        return false;
    }

    @Override
    public int hashCode() {
        String value = toString();
        return value != null ? value.hashCode() : 0;
    }

}
