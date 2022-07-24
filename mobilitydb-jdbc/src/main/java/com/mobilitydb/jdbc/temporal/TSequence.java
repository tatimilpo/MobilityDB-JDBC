package com.mobilitydb.jdbc.temporal;

import com.mobilitydb.jdbc.temporal.delegates.CompareValueFunction;
import com.mobilitydb.jdbc.temporal.delegates.GetTemporalInstantFunction;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;

import java.io.Serializable;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.StringJoiner;

public abstract class TSequence<V extends Serializable> extends TemporalInstants<V> {
    protected boolean stepwise;
    private boolean lowerInclusive;
    private boolean upperInclusive;

    protected TSequence(String value,
                        GetTemporalInstantFunction<V> getTemporalInstantFunction,
                        CompareValueFunction<V> compareValueFunction) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE, compareValueFunction);
        parseValue(value, getTemporalInstantFunction);
        validate();
    }

    protected TSequence(boolean stepwise,
                        String[] values,
                        GetTemporalInstantFunction<V> getTemporalInstantFunction,
                        CompareValueFunction<V> compareValueFunction) throws SQLException {
        this(stepwise, values, true, false, getTemporalInstantFunction, compareValueFunction);
    }

    protected TSequence(boolean stepwise, String[] values, boolean lowerInclusive, boolean upperInclusive,
                        GetTemporalInstantFunction<V> getTemporalInstantFunction,
                        CompareValueFunction<V> compareValueFunction) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE, compareValueFunction);
        for (String val : values) {
            instants.add(getTemporalInstantFunction.run(val.trim()));
        }
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
        this.stepwise = stepwise;
        validate();
    }

    protected TSequence(boolean stepwise, TInstant<V>[] values, CompareValueFunction<V> compareValueFunction)
            throws SQLException {
        this(stepwise, values, true, false, compareValueFunction);
    }

    protected TSequence(boolean stepwise, TInstant<V>[] values, boolean lowerInclusive, boolean upperInclusive,
                        CompareValueFunction<V> compareValueFunction) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE, compareValueFunction);
        instants.addAll(Arrays.asList(values));
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
        this.stepwise = stepwise;
        validate();
    }

    private void parseValue(String value, GetTemporalInstantFunction<V> getTemporalInstantFunction)
            throws SQLException {
        String[] values = preprocessValue(value).split(",");

        // TODO: Investigate if case insensitive comparison is required
        if (values[0].startsWith(TemporalConstants.STEPWISE)) {
            stepwise = true;
            values[0] = values[0].substring(TemporalConstants.STEPWISE.length());
        }

        if (values[0].startsWith(TemporalConstants.LOWER_INCLUSIVE)) {
            this.lowerInclusive = true;
        } else if (values[0].startsWith(TemporalConstants.LOWER_EXCLUSIVE)) {
            this.lowerInclusive = false;
        } else {
            throw new SQLException("Lower bound flag must be either '[' or '('.");
        }

        if (values[values.length - 1].endsWith(TemporalConstants.UPPER_INCLUSIVE)) {
            this.upperInclusive = true;
        } else if (values[values.length - 1].endsWith(TemporalConstants.UPPER_EXCLUSIVE)) {
            this.upperInclusive = false;
        } else {
            throw new SQLException("Upper bound flag must be either ']' or ')'.");
        }

        for (int i = 0; i < values.length; i++) {
            String val = values[i];
            if (i == 0) {
                val = val.substring(1);
            }
            if (i == values.length - 1 ) {
                val = val.substring(0, val.length() - 1);
            }
            instants.add(getTemporalInstantFunction.run(val.trim()));
        }
    }

    @Override
    protected void validateTemporalDataType() throws SQLException {
        // TODO: Implement
    }

    protected boolean explicitInterpolation() {
        return true;
    }

    @Override
    public String buildValue() {
        return buildValue(false);
    }

    String buildValue(boolean skipInterpolation) {
        StringJoiner sj = new StringJoiner(", ");

        for (TInstant<V> temp : instants) {
            sj.add(temp.toString());
        }

        return String.format("%s%s%s%s",
                !skipInterpolation && stepwise && explicitInterpolation() ? TemporalConstants.STEPWISE: "",
                lowerInclusive ? TemporalConstants.LOWER_INCLUSIVE : TemporalConstants.LOWER_EXCLUSIVE,
                sj.toString(),
                upperInclusive ? TemporalConstants.UPPER_INCLUSIVE : TemporalConstants.UPPER_EXCLUSIVE);
    }

    @Override
    public Period period() throws SQLException  {
        return new Period(instants.get(0).getTimestamp(),
                instants.get(instants.size() - 1).getTimestamp(),
                lowerInclusive, upperInclusive);
    }

    @Override
    public PeriodSet getTime() throws SQLException {
        return new PeriodSet(period());
    }

    @Override
    public Duration duration() {
        try {
            return period().duration();
        } catch (SQLException ex) {
            return Duration.ZERO;
        }
    }

    @Override
    public Duration timespan() {
        try {
            return period().duration();
        } catch (SQLException ex) {
            return Duration.ZERO;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() == obj.getClass()) {
            TSequence<?> otherTemporal = (TSequence<?>) obj;

            if (stepwise != otherTemporal.stepwise) {
                return false;
            }

            if (lowerInclusive != otherTemporal.lowerInclusive) {
                return false;
            }

            if (upperInclusive != otherTemporal.upperInclusive) {
                return false;
            }

            return super.equals(obj);
        }
        return false;
    }

    @Override
    public int hashCode() {
        String value = toString();
        return value != null ? value.hashCode() : 0;
    }

    public boolean isStepwise() {
        return stepwise;
    }

    public boolean isLowerInclusive() {
        return lowerInclusive;
    }

    public boolean isUpperInclusive() {
        return upperInclusive;
    }
}
