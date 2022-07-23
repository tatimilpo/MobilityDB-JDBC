package com.mobilitydb.jdbc.temporal;

import java.io.Serializable;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class TSequenceSet<V extends Serializable> extends Temporal<V> {
    protected ArrayList<ArrayList<TemporalValue<V>>> temporalValues = new ArrayList<>();
    private final List<Boolean> lowerInclusive = new ArrayList<>();
    private final List<Boolean> upperInclusive = new ArrayList<>();
    protected boolean stepwise;
    private final CompareValue<V> compareValue;

    protected TSequenceSet(String value,
                           GetSingleTemporalValueFunction<V> getSingleTemporalValue,
                           CompareValue<V> compareValue) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE_SET);
        this.compareValue = compareValue;
        parseValue(value, getSingleTemporalValue);
        validate();
    }

    protected TSequenceSet(boolean stepwise,
                           String[] values,
                           GetSingleTemporalValueFunction<V> getSingleTemporalValue,
                           CompareValue<V> compareValue) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE_SET);
        this.compareValue = compareValue;
        this.stepwise = stepwise;
        for (String val : values) {
            parseSequence(val, getSingleTemporalValue);
        }
        validate();
    }

    protected TSequenceSet(boolean stepwise,
                           TSequence<V>[] values,
                           GetSingleTemporalValueFunction<V> getSingleTemporalValue,
                           CompareValue<V> compareValue) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE_SET);
        this.compareValue = compareValue;
        this.stepwise = stepwise;
        for (TSequence<V> val: values) {
            validateSequence(val);
            parseSequence(val.buildValue(true), getSingleTemporalValue);
        }
        validate();
    }

    @Override
    protected void validateTemporalDataType() throws SQLException {
        // TODO: Implement
    }

    @Override
    public String buildValue() {
        StringJoiner sj = new StringJoiner(", ");
        for (int i = 0; i < temporalValues.size(); i++) {
            List<TemporalValue<V>> tempList = temporalValues.get(i);
            StringJoiner sjt = new StringJoiner(", ");
            for (TemporalValue<V> temp : tempList) {
                sjt.add(String.format("%s", temp.toString()));
            }
            sj.add(String.format("%s%s%s",
                    Boolean.TRUE.equals(lowerInclusive.get(i))
                            ? TemporalConstants.LOWER_INCLUSIVE
                            : TemporalConstants.LOWER_EXCLUSIVE,
                    sjt.toString(),
                    Boolean.TRUE.equals(upperInclusive.get(i))
                            ? TemporalConstants.UPPER_INCLUSIVE
                            : TemporalConstants.UPPER_EXCLUSIVE));
        }
        return String.format("%s{%s}",
                stepwise && explicitInterpolation() ? TemporalConstants.STEPWISE: "",
                sj.toString());
    }

    private void parseValue(String value, GetSingleTemporalValueFunction<V> getSingleTemporalValue)
            throws SQLException {
        String newValue = preprocessValue(value);

        // TODO: Investigate if case insensitive comparison is required
        if (newValue.startsWith(TemporalConstants.STEPWISE)) {
            stepwise = true;
            newValue = newValue.substring(TemporalConstants.STEPWISE.length());
        }

        newValue = newValue.replace("{", "").replace("}", "").trim();
        Matcher m = Pattern.compile("[\\[|\\(].*?[^\\]\\)][\\]|\\)]")
                .matcher(newValue);
        List<String> seqValues = new ArrayList<>();
        while (m.find()) {
            seqValues.add(m.group());
        }
        for (String seq : seqValues) {
            parseSequence(seq, getSingleTemporalValue);
        }
    }

    private void validateSequence(TSequence<V> sequence) throws SQLException {
        if (sequence == null) {
            throw new SQLException("Sequence cannot be null.");
        }

        if (sequence.stepwise != this.stepwise) {
            throw new SQLException("Sequence should have the same interpolation.");
        }
    }

    private void parseSequence(String value, GetSingleTemporalValueFunction<V> getSingleTemporalValue)
            throws SQLException {
        String[] values = value.split(",");

        if (values[0].startsWith(TemporalConstants.LOWER_INCLUSIVE)) {
            this.lowerInclusive.add(true);
        } else if (values[0].startsWith(TemporalConstants.LOWER_EXCLUSIVE)) {
            this.lowerInclusive.add(false);
        } else {
            throw new SQLException("Lower bound flag must be either '[' or '('.");
        }

        if (values[values.length - 1].endsWith(TemporalConstants.UPPER_INCLUSIVE)) {
            this.upperInclusive.add(true);
        } else if (values[values.length - 1].endsWith(TemporalConstants.UPPER_EXCLUSIVE)) {
            this.upperInclusive.add(false);
        } else {
            throw new SQLException("Upper bound flag must be either ']' or ')'.");
        }
        ArrayList<TemporalValue<V>> temp = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            String val = values[i];
            if (i == 0) {
                val = val.substring(1);
            }
            if (i == values.length - 1) {
                val = val.substring(0, val.length() - 1);
            }
            temp.add(getSingleTemporalValue.run(val.trim()));
        }
        temporalValues.add(temp);
    }

    protected boolean explicitInterpolation() {
        return true;
    }

    @Override
    public List<V> getValues() {
        List<V> values = new ArrayList<>();
        for (List<TemporalValue<V>> tempList : temporalValues) {
            for (TemporalValue<V> temp : tempList) {
                values.add(temp.getValue());
            }
        }
        return values;
    }

    @Override
    public V startValue() {
        return temporalValues.get(0).get(0).getValue();
    }

    @Override
    public V endValue() {
        ArrayList<TemporalValue<V>> last = temporalValues.get(temporalValues.size() - 1);
        return last.get(last.size() - 1).getValue();
    }

    @Override
    public V minValue() {
        // TODO improve
        List<V> values = getValues();
        V min = values.get(0);
        for (V value : values) {
            if (compareValue.run(value, min) < 0) {
                min = value;
            }
        }
        return min;
    }

    @Override
    public V maxValue() {
        // TODO improve
        List<V> values = getValues();
        V max = values.get(0);
        for (V value : values) {
            if (compareValue.run(value, max) > 0) {
                max = value;
            }
        }
        return max;
    }

    @Override
    public V valueAtTimestamp(OffsetDateTime timestamp) {
        for (List<TemporalValue<V>> tempList : temporalValues) {
            for (TemporalValue<V> temp : tempList) {
                if (timestamp.isEqual(temp.getTime())) {
                    return temp.getValue();
                }
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        TSequenceSet<?> otherTemporal = (TSequenceSet<?>) obj;

        if (this.stepwise != otherTemporal.stepwise) {
            return false;
        }

        if (this.temporalValues.size() != otherTemporal.temporalValues.size()) {
            return false;
        }

        for (int i = 0; i < this.temporalValues.size(); i++) {
            boolean areEqual = lowerInclusive.get(i) == otherTemporal.lowerInclusive.get(i);
            areEqual = areEqual && upperInclusive.get(i) == otherTemporal.upperInclusive.get(i);
            areEqual = areEqual && temporalValues.get(i).size() == otherTemporal.temporalValues.get(i).size();

            if (!areEqual) {
                return false;
            }

            for (int j = 0; j < temporalValues.get(i).size(); j++) {
                TemporalValue<V> thisVal = temporalValues.get(i).get(j);
                TemporalValue<?> otherVal = otherTemporal.temporalValues.get(i).get(j);
                if (!thisVal.equals(otherVal)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        String value = toString();
        return value != null ? value.hashCode() : 0;
    }
}
