package com.mobilitydb.jdbc.temporal;

import com.mobilitydb.jdbc.temporal.delegates.CompareValueFunction;
import com.mobilitydb.jdbc.temporal.delegates.GetTemporalSequenceFunction;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.PeriodSet;

import java.io.Serializable;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class TSequenceSet<V extends Serializable> extends Temporal<V> implements TemporalSequences<V> {
    protected ArrayList<TSequence<V>> sequenceList = new ArrayList<>();
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
                           String value,
                           GetTemporalSequenceFunction<V> getTemporalSequenceFunction,
                           CompareValueFunction<V> compareValueFunction) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE_SET);
        this.compareValueFunction = compareValueFunction;
        parseValue(value, getTemporalSequenceFunction);
        this.stepwise = stepwise;
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
            sequenceList.add(sequence);
        }
        validate();
    }

    protected TSequenceSet(boolean stepwise,
                           TSequence<V>[] values,
                           CompareValueFunction<V> compareValueFunction) throws SQLException {
        super(TemporalType.TEMPORAL_SEQUENCE_SET);
        this.compareValueFunction = compareValueFunction;
        this.stepwise = stepwise;
        sequenceList.addAll(Arrays.asList(values));
        validate();
    }

    @Override
    protected void validateTemporalDataType() throws SQLException {
        if (sequenceList.isEmpty()) {
            throw new SQLException("Sequence set must be composed of at least one sequence.");
        }

        for (int i = 0; i < sequenceList.size(); i++) {
            TSequence<V> x = sequenceList.get(i);
            validateSequence(x);

            if (i + 1 < sequenceList.size()) {
                TSequence<V>  y = sequenceList.get(i + 1);
                validateSequence(y);

                if (x.endTimestamp().isAfter(y.startTimestamp()) ||
                    (x.endTimestamp().isEqual(y.startTimestamp()) && x.isUpperInclusive() && y.isLowerInclusive())) {
                    throw new SQLException("The sequences of a sequence set cannot overlap.");
                }
            }
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

    @Override
    public String buildValue() {
        StringJoiner sj = new StringJoiner(", ");

        for (TSequence<V> sequence : sequenceList) {
            sj.add(sequence.buildValue(true));
        }

        return String.format("%s{%s}",
                stepwise && explicitInterpolation() ? TemporalConstants.STEPWISE: "",
                sj.toString());
    }

    private void parseValue(String value, GetTemporalSequenceFunction<V> getTemporalSequenceFunction)
            throws SQLException {
        String newValue = preprocessValue(value);

        if (newValue.startsWith(TemporalConstants.STEPWISE)) {
            stepwise = true;
            newValue = newValue.substring(TemporalConstants.STEPWISE.length());
        }

        newValue = newValue.replace("{", "").replace("}", "").trim();
        List<String> seqValues = getSequenceValues(newValue);

        for (String seq : seqValues) {
            if (stepwise && !seq.startsWith(TemporalConstants.STEPWISE)) {
                seq = TemporalConstants.STEPWISE + seq;
            }
            sequenceList.add(getTemporalSequenceFunction.run(seq));
        }
    }

    protected List<String> getSequenceValues(String value) {
        Matcher m = Pattern.compile("[\\[|\\(].*?[^\\]\\)][\\]|\\)]")
                .matcher(value);
        List<String> seqValues = new ArrayList<>();
        while (m.find()) {
            seqValues.add(m.group());
        }
        return seqValues;
    }

    protected boolean explicitInterpolation() {
        return true;
    }

    @Override
    public List<V> getValues() {
        List<V> values = new ArrayList<>();
        for (TSequence<V> sequence : sequenceList) {
            values.addAll(sequence.getValues());
        }
        return values;
    }

    @Override
    public V startValue() {
        if (sequenceList.isEmpty()) {
            return null;
        }

        return sequenceList.get(0).startValue();
    }

    @Override
    public V endValue() {
        if (sequenceList.isEmpty()) {
            return null;
        }

        return sequenceList.get(sequenceList.size() - 1).endValue();
    }

    @Override
    public V minValue() {
        if (sequenceList.isEmpty()) {
            return null;
        }

        V min = null;

        for (TSequence<V> sequence : sequenceList) {
            V value = sequence.minValue();

            if (min == null || compareValueFunction.run(value, min) < 0) {
                min = value;
            }
        }

        return min;
    }

    @Override
    public V maxValue() {
        if (sequenceList.isEmpty()) {
            return null;
        }

        V max = null;

        for (TSequence<V> sequence : sequenceList) {
            V value = sequence.maxValue();

            if (max == null || compareValueFunction.run(value, max) > 0) {
                max = value;
            }
        }

        return max;
    }

    @Override
    public V valueAtTimestamp(OffsetDateTime timestamp) {
        for (TSequence<V> sequence : sequenceList) {
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

        for (TSequence<V> sequence : sequenceList) {
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

        throw new SQLException("There is no timestamp at this index.");
    }

    @Override
    public OffsetDateTime startTimestamp() {
        return sequenceList.get(0).startTimestamp();
    }

    @Override
    public OffsetDateTime endTimestamp() {
        return sequenceList.get(sequenceList.size() - 1).endTimestamp();
    }

    @Override
    public Period period() throws SQLException {
        TSequence<V> first = sequenceList.get(0);
        TSequence<V> last = sequenceList.get(sequenceList.size() - 1);
        return new Period(first.startTimestamp(), last.endTimestamp(),
                first.isLowerInclusive(), last.isUpperInclusive());
    }

    @Override
    public PeriodSet getTime() throws SQLException {
        ArrayList<Period> periods = new ArrayList<>();
        for (TSequence<V> sequence : sequenceList) {
            periods.add(sequence.period());
        }
        return new PeriodSet(periods.toArray(new Period[0]));
    }

    @Override
    public int numInstants() {
        return instants().size();
    }

    @Override
    public TInstant<V> startInstant() {
        return sequenceList.get(0).startInstant();
    }

    @Override
    public TInstant<V> endInstant() {
        return sequenceList.get(sequenceList.size() - 1).endInstant();
    }

    @Override
    public TInstant<V> instantN(int n) throws SQLException {
        List<TInstant<V>> instants = instants();

        if (n >= 0 && n < instants.size()) {
            return instants.get(n);
        }

        throw new SQLException("There is no instant at this index.");
    }

    @Override
    public List<TInstant<V>> instants() {
        ArrayList<TInstant<V>> list = new ArrayList<>();
        for (TSequence<V> sequence : sequenceList) {
            list.addAll(sequence.instantList);
        }
        return list;
    }

    @Override
    public Duration duration() {
        Duration duration = Duration.ZERO;
        for (TSequence<V> sequence : sequenceList) {
            duration = duration.plus(sequence.duration());
        }
        return duration;
    }

    @Override
    public Duration timespan() {
        return Duration.between(startTimestamp(), endTimestamp());
    }

    @Override
    public void shift(Duration duration) {
        for (TSequence<V> sequence : sequenceList) {
            sequence.shift(duration);
        }
    }

    @Override
    public boolean intersectsTimestamp(OffsetDateTime dateTime) {
        for (TSequence<V> sequence : sequenceList) {
            if (sequence.intersectsTimestamp(dateTime)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean intersectsPeriod(Period period) {
        for (TSequence<V> sequence : sequenceList) {
            if (sequence.intersectsPeriod(period)) {
                return true;
            }
        }
        return false;
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

        if (this.sequenceList.size() != otherTemporal.sequenceList.size()) {
            return false;
        }

        for (int i = 0; i < this.sequenceList.size(); i++) {
            TSequence<V> thisVal = sequenceList.get(i);
            TSequence<?> otherVal = otherTemporal.sequenceList.get(i);

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

    @Override
    public int numSequences() {
        return sequenceList.size();
    }

    @Override
    public TSequence<V> startSequence() {
        if (sequenceList.isEmpty()) {
            return null;
        }

        return sequenceList.get(0);
    }

    @Override
    public TSequence<V> endSequence() {
        if (sequenceList.isEmpty()) {
            return null;
        }

        return sequenceList.get(sequenceList.size() - 1);
    }

    @Override
    public TSequence<V> sequenceN(int n) throws SQLException {
        if (n >= 0 && n < sequenceList.size()) {
            return sequenceList.get(n);
        }

        throw new SQLException("There is no sequence at this index.");
    }

    @Override
    public List<TSequence<V>> sequences() {
        return new ArrayList<>(sequenceList);
    }
}
