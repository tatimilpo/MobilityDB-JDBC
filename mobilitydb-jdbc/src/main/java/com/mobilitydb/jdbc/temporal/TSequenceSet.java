package com.mobilitydb.jdbc.temporal;

import com.mobilitydb.jdbc.temporal.delegates.CompareValueFunction;
import com.mobilitydb.jdbc.temporal.delegates.GetTemporalSequenceFunction;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;

import java.io.Serializable;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class TSequenceSet<V extends Serializable> extends Temporal<V> {
    protected ArrayList<TSequence<V>> sequences = new ArrayList<>();
    protected boolean stepwise;
    private final CompareValueFunction<V> compareValueFunction;

    protected TSequenceSet(String value,
                           GetTemporalSequenceFunction<V> getTemporalSequenceFunction,
                           CompareValueFunction<V> compareValueFunction) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE_SET);
        this.compareValueFunction = compareValueFunction;
        parseValue(value, getTemporalSequenceFunction);
        validate();
    }

    protected TSequenceSet(boolean stepwise,
                           String[] values,
                           GetTemporalSequenceFunction<V> getTemporalSequenceFunction,
                           CompareValueFunction<V> compareValueFunction) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE_SET);
        this.compareValueFunction = compareValueFunction;
        this.stepwise = stepwise;
        for (String val : values) {
            TSequence<V> sequence = getTemporalSequenceFunction.run(val);
            validateSequence(sequence);
            sequences.add(sequence);
        }
        validate();
    }

    protected TSequenceSet(boolean stepwise,
                           TSequence<V>[] values,
                           CompareValueFunction<V> compareValueFunction) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE_SET);
        this.compareValueFunction = compareValueFunction;
        this.stepwise = stepwise;
        for (TSequence<V> sequence: values) {
            validateSequence(sequence);
            sequences.add(sequence);
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

        for (TSequence<V> sequence : sequences) {
            sj.add(sequence.buildValue(true));
        }

        return String.format("%s{%s}",
                stepwise && explicitInterpolation() ? TemporalConstants.STEPWISE: "",
                sj.toString());
    }

    private void parseValue(String value, GetTemporalSequenceFunction<V> getTemporalSequenceFunction)
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
            if (stepwise && !seq.startsWith(TemporalConstants.STEPWISE)) {
                seq = TemporalConstants.STEPWISE + seq;
            }
            sequences.add(getTemporalSequenceFunction.run(seq));
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

    protected boolean explicitInterpolation() {
        return true;
    }

    @Override
    public List<V> getValues() {
        List<V> values = new ArrayList<>();
        for (TSequence<V> sequence : sequences) {
            values.addAll(sequence.getValues());
        }
        return values;
    }

    @Override
    public V startValue() {
        if (sequences.isEmpty()) {
            return null;
        }

        return sequences.get(0).startValue();
    }

    @Override
    public V endValue() {
        if (sequences.isEmpty()) {
            return null;
        }

        return sequences.get(sequences.size() - 1).endValue();
    }

    @Override
    public V minValue() {
        if (sequences.isEmpty()) {
            return null;
        }

        V min = null;

        for (TSequence<V> sequence : sequences) {
            V value = sequence.minValue();

            if (min == null || compareValueFunction.run(value, min) < 0) {
                min = value;
            }
        }

        return min;
    }

    @Override
    public V maxValue() {
        if (sequences.isEmpty()) {
            return null;
        }

        V max = null;

        for (TSequence<V> sequence : sequences) {
            V value = sequence.maxValue();

            if (max == null || compareValueFunction.run(value, max) > 0) {
                max = value;
            }
        }

        return max;
    }

    @Override
    public V valueAtTimestamp(OffsetDateTime timestamp) {
        for (TSequence<V> sequence : sequences) {
            V value = sequence.valueAtTimestamp(timestamp);

            if (value != null) {
                return value;
            }
        }
        return null;
    }

    @Override
    public int numTimestamps() {
        return timestamps().length;
    }

    @Override
    public OffsetDateTime[] timestamps() {
        LinkedHashSet<OffsetDateTime> timestamps = new LinkedHashSet<>();

        for (TSequence<V> sequence : sequences) {
            timestamps.addAll(Arrays.asList(sequence.timestamps()));
        }

        return timestamps.toArray(new OffsetDateTime[0]);
    }

    @Override
    public OffsetDateTime timestampN(int n) throws SQLException {
        OffsetDateTime[] timestamps = timestamps();
        if (n >= 0 && n < timestamps.length) {
            return timestamps[n];
        }

        throw new SQLException("There is no value at this index.");
    }

    @Override
    public OffsetDateTime startTimestamp() {
        return sequences.get(0).startTimestamp();
    }

    @Override
    public OffsetDateTime endTimestamp() {
        return sequences.get(sequences.size() - 1).endTimestamp();
    }

    @Override
    public Period period() throws SQLException {
        TSequence<V> first = sequences.get(0);
        TSequence<V> last = sequences.get(sequences.size() - 1);
        return new Period(first.startTimestamp(), last.endTimestamp(),
                first.isLowerInclusive(), last.isUpperInclusive());
    }

    @Override
    public PeriodSet getTime() throws SQLException {
        ArrayList<Period> periods = new ArrayList<>();
        for (TSequence<V> sequence : sequences) {
            periods.add(sequence.period());
        }
        return new PeriodSet(periods.toArray(new Period[0]));
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

        if (this.sequences.size() != otherTemporal.sequences.size()) {
            return false;
        }

        for (int i = 0; i < this.sequences.size(); i++) {
            TSequence<V> thisVal = sequences.get(i);
            TSequence<?> otherVal = otherTemporal.sequences.get(i);

            if (!thisVal.equals(otherVal)) {
                return false;
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
