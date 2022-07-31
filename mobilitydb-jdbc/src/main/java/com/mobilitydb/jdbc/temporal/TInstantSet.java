package com.mobilitydb.jdbc.temporal;

import com.mobilitydb.jdbc.temporal.delegates.CompareValueFunction;
import com.mobilitydb.jdbc.temporal.delegates.GetTemporalInstantFunction;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;

import java.io.Serializable;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringJoiner;

public abstract class TInstantSet<V extends Serializable> extends TemporalInstants<V> {
    protected TInstantSet(String value,
                          GetTemporalInstantFunction<V> getTemporalInstantFunction,
                          CompareValueFunction<V> compareValueFunction) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT_SET, compareValueFunction);
        parseValue(value, getTemporalInstantFunction);
        validate();
    }

    protected TInstantSet(String[] values,
                          GetTemporalInstantFunction<V> getTemporalInstantFunction,
                          CompareValueFunction<V> compareValueFunction) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT_SET, compareValueFunction);
        for (String val : values) {
            instantList.add(getTemporalInstantFunction.run(val.trim()));
        }
        validate();
    }

    protected TInstantSet(TInstant<V>[] values, CompareValueFunction<V> compareValueFunction) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT_SET, compareValueFunction);
        instantList.addAll(Arrays.asList(values));
        validate();
    }

    @Override
    protected void validateTemporalDataType() throws SQLException {
        validateInstantList("Temporal instant set");
    }

    @Override
    public Period period() throws SQLException  {
        return new Period(instantList.get(0).getTimestamp(),
                instantList.get(instantList.size() - 1).getTimestamp(),
                true, true);
    }

    @Override
    public PeriodSet getTime() throws SQLException {
        ArrayList<Period> periods = new ArrayList<>();
        for (TInstant<V> instant : instantList) {
            periods.add(instant.period());
        }
        return new PeriodSet(periods.toArray(new Period[0]));
    }

    @Override
    public Duration duration() {
        return Duration.ZERO;
    }

    @Override
    public Duration timespan() {
        return Duration.between(startTimestamp(), endTimestamp());
    }

    @Override
    public boolean intersectsTimestamp(OffsetDateTime dateTime) {
        for (TInstant<V> instant : instantList) {
            if (instant.intersectsTimestamp(dateTime)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean intersectsPeriod(Period period) {
        for (TInstant<V> instant : instantList) {
            if (instant.intersectsPeriod(period)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String buildValue() {
        StringJoiner sj = new StringJoiner(", ");
        for (TInstant<V> temp : instantList) {
            sj.add(temp.toString());
        }
        return String.format("{%s}", sj.toString());
    }

    private void parseValue(String value, GetTemporalInstantFunction<V> getTemporalFunction) throws SQLException {
        String newValue = preprocessValue(value);
        newValue = newValue.replace("{", "").replace("}", "");
        String[] values = newValue.split(",", -1);
        for (String val : values) {
            instantList.add(getTemporalFunction.run(val.trim()));
        }
    }
}
