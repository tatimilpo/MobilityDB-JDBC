package com.mobilitydb.jdbc.temporal;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.StringJoiner;

public abstract class TSequence<V extends Serializable> extends TemporalInstants<V> {
    protected boolean stepwise;
    private boolean lowerInclusive;
    private boolean upperInclusive;

    protected TSequence(String value,
                        GetSingleTemporalValueFunction<V> getSingleTemporalValue,
                        CompareValue<V> compareValue) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE, compareValue);
        parseValue(value, getSingleTemporalValue);
        validate();
    }

    protected TSequence(boolean stepwise,
                        String[] values,
                        GetSingleTemporalValueFunction<V> getSingleTemporalValue,
                        CompareValue<V> compareValue) throws SQLException {
        this(stepwise, values, true, false, getSingleTemporalValue, compareValue);
    }

    protected TSequence(boolean stepwise, String[] values, boolean lowerInclusive, boolean upperInclusive,
                        GetSingleTemporalValueFunction<V> getSingleTemporalValue,
                        CompareValue<V> compareValue) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE, compareValue);
        for (String val : values) {
            temporalValues.add(getSingleTemporalValue.run(val.trim()));
        }
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
        this.stepwise = stepwise;
        validate();
    }

    protected TSequence(boolean stepwise, TInstant<V>[] values, CompareValue<V> compareValue) throws SQLException {
        this(stepwise, values, true, false, compareValue);
    }

    protected TSequence(boolean stepwise, TInstant<V>[] values, boolean lowerInclusive, boolean upperInclusive,
                        CompareValue<V> compareValue) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE, compareValue);
        for (TInstant<V> val : values) {
            temporalValues.add(val.getTemporalValue());
        }
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
        this.stepwise = stepwise;
        validate();
    }

    private void parseValue(String value, GetSingleTemporalValueFunction<V> getSingleTemporalValue)
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
            temporalValues.add(getSingleTemporalValue.run(val.trim()));
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

        for (TemporalValue<V> temp : temporalValues) {
            sj.add(temp.toString());
        }

        return String.format("%s%s%s%s",
                !skipInterpolation && stepwise && explicitInterpolation() ? TemporalConstants.STEPWISE: "",
                lowerInclusive ? TemporalConstants.LOWER_INCLUSIVE : TemporalConstants.LOWER_EXCLUSIVE,
                sj.toString(),
                upperInclusive ? TemporalConstants.UPPER_INCLUSIVE : TemporalConstants.UPPER_EXCLUSIVE);
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
}
