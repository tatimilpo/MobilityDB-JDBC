package com.mobilitydb.jdbc.temporal;

import com.mobilitydb.jdbc.temporal.delegates.GetSingleTemporalValueFunction;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;

import java.io.Serializable;
import java.sql.SQLException;
import java.time.Duration;
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

    protected TInstant(TemporalValue<V> value) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT);
        temporalValue = value;
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

    @Override
    public int numTimestamps() {
        return 1;
    }

    @Override
    public OffsetDateTime[] timestamps() {
        return new OffsetDateTime[] { temporalValue.getTime() };
    }

    @Override
    public OffsetDateTime timestampN(int n) throws SQLException {
        if (n == 0) {
            return temporalValue.getTime();
        }

        throw new SQLException("There is no timestamp at this index.");
    }

    @Override
    public OffsetDateTime startTimestamp() {
        return temporalValue.getTime();
    }

    @Override
    public OffsetDateTime endTimestamp() {
        return temporalValue.getTime();
    }

    @Override
    public Period period() throws SQLException  {
        return new Period(temporalValue.getTime(), temporalValue.getTime(), true, true);
    }

    @Override
    public PeriodSet getTime() throws SQLException {
        return new PeriodSet(period());
    }

    @Override
    public int numInstants() {
        return 1;
    }

    @Override
    public TInstant<V> startInstant() {
        return this;
    }

    @Override
    public TInstant<V> endInstant() {
        return this;
    }

    @Override
    public TInstant<V> instantN(int n) throws SQLException {
        if (n == 0) {
            return this;
        }

        throw new SQLException("There is no instant at this index.");
    }

    @Override
    public List<TInstant<V>> instants() {
        ArrayList<TInstant<V>> list = new ArrayList<>();
        list.add(this);
        return list;
    }

    @Override
    public Duration duration() {
        return Duration.ZERO;
    }

    @Override
    public Duration timespan() {
        return Duration.ZERO;
    }

    @Override
    public void shift(Duration duration) {
        temporalValue.setTime(temporalValue.getTime().plus(duration));
    }

    @Override
    public boolean intersectsTimestamp(OffsetDateTime dateTime) {
        return temporalValue.getTime().isEqual(dateTime);
    }

    @Override
    public boolean intersectsPeriod(Period period) {
        if (period == null) {
            return false;
        }

        return period.contains(temporalValue.getTime());
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
