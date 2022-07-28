package com.mobilitydb.jdbc.temporal;

import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;

import java.io.Serializable;
import java.sql.SQLException;
import java.time.Duration;
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

    public abstract int numTimestamps();

    public abstract OffsetDateTime[] timestamps();

    public abstract OffsetDateTime timestampN(int n) throws SQLException;

    public abstract OffsetDateTime startTimestamp();

    public abstract OffsetDateTime endTimestamp();

    public abstract PeriodSet getTime() throws SQLException;

    public abstract Period period() throws SQLException;

    public abstract int numInstants();

    public abstract TInstant<V> startInstant();

    public abstract TInstant<V> endInstant();

    public abstract TInstant<V> instantN(int n) throws SQLException;

    public abstract List<TInstant<V>> instants();

    public abstract Duration duration();

    public abstract Duration timespan();

    public abstract void shift(Duration duration);

    public abstract boolean intersectsTimestamp(OffsetDateTime dateTime);

    public abstract boolean intersectsPeriod(Period period);

    public TemporalType getTemporalType() {
        return temporalType;
    }

    @Override
    public String toString() {
        return buildValue();
    }
}
