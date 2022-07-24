package com.mobilitydb.jdbc.temporal;

import com.mobilitydb.jdbc.temporal.delegates.CompareValueFunction;
import com.mobilitydb.jdbc.temporal.delegates.GetTemporalInstantFunction;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;

import java.io.Serializable;
import java.sql.SQLException;
import java.time.Duration;
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
            instants.add(getTemporalInstantFunction.run(val.trim()));
        }
        validate();
    }

    protected TInstantSet(TInstant<V>[] values, CompareValueFunction<V> compareValueFunction) throws SQLException {
        super(TemporalType.TEMPORAL_INSTANT_SET, compareValueFunction);
        instants.addAll(Arrays.asList(values));
        validate();
    }

    @Override
    protected void validateTemporalDataType() throws SQLException {
        // TODO: Implement
    }

    @Override
    public Period period() throws SQLException  {
        return new Period(instants.get(0).getTimestamp(),
                instants.get(instants.size() - 1).getTimestamp(),
                true, true);
    }

    @Override
    public PeriodSet getTime() throws SQLException {
        ArrayList<Period> periods = new ArrayList<>();
        for (TInstant<V> instant : instants) {
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
    public String buildValue() {
        StringJoiner sj = new StringJoiner(", ");
        for (TInstant<V> temp : instants) {
            sj.add(temp.toString());
        }
        return String.format("{%s}", sj.toString());
    }

    private void parseValue(String value, GetTemporalInstantFunction<V> getTemporalFunction) throws SQLException {
        String newValue = preprocessValue(value);
        newValue = newValue.replace("{", "").replace("}", "");
        String[] values = newValue.split(",", -1);
        for (String val : values) {
            instants.add(getTemporalFunction.run(val.trim()));
        }
    }
}
