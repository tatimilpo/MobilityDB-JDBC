package com.mobilitydb.jdbc.temporal;

import java.io.Serializable;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.List;

/**
 * Wraps a Temporal data type
 * @param <V> - Base type of the temporal data type eg. Integer, Boolean
 */
public abstract class Temporal<V extends Serializable> implements Serializable {
    protected TemporalType temporalType;

    protected Temporal(TemporalType temporalType) {
        this.temporalType = temporalType;
    }

    protected void validate() throws SQLException {
        validateTemporalDataType();
    }

    protected TemporalValue<V> buildTemporalValue(V value, OffsetDateTime time) {
        return new TemporalValue<>(value, time);
    }

    protected String preprocessValue(String value) throws SQLException {
        if (value == null || value.isEmpty()) {
            throw new SQLException("Value cannot be empty.");
        }

        return value;
    }

    protected abstract int compare(V first, V second);

    /**
     * Throws an SQLException if Temporal data type is not valid
     * @throws SQLException
     */
    protected abstract void validateTemporalDataType() throws SQLException;

    public abstract String buildValue();

    public abstract List<V> getValues();

    public abstract V startValue();

    public abstract V endValue();

    public abstract V minValue();

    public abstract V maxValue();

    public abstract V valueAtTimestamp(OffsetDateTime timestamp);

    public TemporalType getTemporalType() {
        return temporalType;
    }

    @Override
    public String toString() {
        return buildValue();
    }
}
