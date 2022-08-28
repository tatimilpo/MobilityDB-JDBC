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

    /**
     * Verifies that the value is not null or empty
     * @param value - a string with the value
     * @return a string
     * @throws SQLException
     */
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

    /**
     * Gets all values
     * @return a list of V values
     */
    public abstract List<V> getValues();

    /**
     * Gets the first value
     * @return a value type V
     */
    public abstract V startValue();

    /**
     * Gets the last value
     * @return a value type V
     */
    public abstract V endValue();

    /**
     * Gets the minimum value
     * @return a value type V
     */
    public abstract V minValue();

    /**
     * Gets the maximum value
     * @return a value type V
     */
    public abstract V maxValue();

    /**
     * Gets the value in the given timestamp
     * @param timestamp - the timestamp
     * @return
     */
    public abstract V valueAtTimestamp(OffsetDateTime timestamp);

    /**
     * Get the number of timestamps
     * @return a number
     */
    public abstract int numTimestamps();

    /**
     * Get all timestamps
     * @return an array with the timestamps
     */
    public abstract OffsetDateTime[] timestamps();

    /**
     * Gets the timestamp located at the index position
     * @param n - the index
     * @return a timestamp
     * @throws SQLException
     */
    public abstract OffsetDateTime timestampN(int n) throws SQLException;

    /**
     * Gets the first timestamp
     * @return a timestamp
     */
    public abstract OffsetDateTime startTimestamp();

    /**
     * Gets the last timestamp
     * @return a timestamp
     */
    public abstract OffsetDateTime endTimestamp();

    /**
     * Gets the periodset on which the temporal value is defined
     * @return a Periodset
     * @throws SQLException
     */
    public abstract PeriodSet getTime() throws SQLException;

    /**
     * Gets the period
     * @return a Period
     * @throws SQLException
     */
    public abstract Period period() throws SQLException;

    /**
     * Gets the number of instants
     * @return a number
     */
    public abstract int numInstants();

    /**
     * Gets the first instant
     * @return a temporal instant type V
     */
    public abstract TInstant<V> startInstant();

    /**
     * Gets the last instant
     * @return a temporal instant type V
     */
    public abstract TInstant<V> endInstant();

    /**
     * Gets the instant in the given index
     * @param n - the index
     * @return a temporal instant type V
     * @throws SQLException
     */
    public abstract TInstant<V> instantN(int n) throws SQLException;

    /**
     * Gets all temporal instants
     * @return a list of temporal instants type V
     */
    public abstract List<TInstant<V>> instants();

    /**
     * Gets the interval on which the temporal value is defined
     * @return a duration
     */
    public abstract Duration duration();

    /**
     * Gets the interval on which the temporal value is defined ignoring the potential time gaps
     * @return a duration
     */
    public abstract Duration timespan();

    /**
     * Shifts the duration sent
     * @param duration - the duration to shift
     */
    public abstract void shift(Duration duration);

    /**
     * If the temporal value intersects the timestamp sent
     * @param dateTime - the timestamp
     * @return true if the timestamp intersects, otherwise false
     */
    public abstract boolean intersectsTimestamp(OffsetDateTime dateTime);

    /**
     * If the temporal value intersects the Period sent
     * @param period - the period
     * @return true if the period intersects, otherwise false
     */
    public abstract boolean intersectsPeriod(Period period);

    /**
     * Gets the temporal type
     * @return a TemporalType
     */
    public TemporalType getTemporalType() {
        return temporalType;
    }

    @Override
    public String toString() {
        return buildValue();
    }
}
