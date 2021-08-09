package edu.ulb.mobilitydb.jdbc.time;

import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;

import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

@TypeName(name = "period")
public class Period extends DataType {
    private OffsetDateTime lower;
    private OffsetDateTime upper;
    private boolean lowerInclusive;
    private boolean upperInclusive;

    private static final String FORMAT = "yyyy-MM-dd HH:mm:ssX";
    private static final String LOWER_INCLUSIVE = "[";
    private static final String LOWER_EXCLUSIVE = "(";
    private static final String UPPER_INCLUSIVE = "]";
    private static final String UPPER_EXCLUSIVE = ")";

    public Period() {
        super();
    }

    public Period(final String value) throws SQLException {
        super();
        setValue(value);
    }

    public Period(OffsetDateTime lower, OffsetDateTime upper) throws SQLException {
        super();
        this.lower = lower;
        this.upper = upper;
        this.lowerInclusive = true;
        this.upperInclusive = false;
        validate();
    }

    public Period(OffsetDateTime lower, OffsetDateTime upper, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super();
        this.lower = lower;
        this.upper = upper;
        this.lowerInclusive = lowerInclusive;
        this.upperInclusive = upperInclusive;
        validate();
    }

    /** {@inheritDoc} */
    @Override
    public String getValue() {
        if (lower == null || upper == null) {
            return null;
        }

        DateTimeFormatter format = DateTimeFormatter.ofPattern(FORMAT);

        return String.format("%s%s, %s%s",
                lowerInclusive ? LOWER_INCLUSIVE : LOWER_EXCLUSIVE,
                format.format(lower),
                format.format(upper),
                upperInclusive ? UPPER_INCLUSIVE : UPPER_EXCLUSIVE);
    }

    public OffsetDateTime getLower() {
        return lower;
    }

    public OffsetDateTime getUpper() {
        return upper;
    }

    public boolean isLowerInclusive() {
        return lowerInclusive;
    }

    public boolean isUpperInclusive() {
        return upperInclusive;
    }

    /** {@inheritDoc} */
    @Override
    public void setValue(final String value) throws SQLException {
        String[] values = value.split(",");
        DateTimeFormatter format = DateTimeFormatter.ofPattern(FORMAT);

        if (values.length != 2) {
            throw new SQLException("Could not parse period value.");
        }

        if (values[0].startsWith(LOWER_INCLUSIVE)) {
            this.lowerInclusive = true;
        } else if (values[0].startsWith(LOWER_EXCLUSIVE)) {
            this.lowerInclusive = false;
        } else {
            throw new SQLException("Lower bound flag must be either '[' or '('.");
        }

        if (values[1].endsWith(UPPER_INCLUSIVE)) {
            this.upperInclusive = true;
        } else if (values[1].endsWith(UPPER_EXCLUSIVE)) {
            this.upperInclusive = false;
        } else {
            throw new SQLException("Upper bound flag must be either ']' or ')'.");
        }

        this.lower = OffsetDateTime.parse(values[0].substring(1).trim(), format);
        this.upper = OffsetDateTime.parse(values[1].substring(0, values[1].length() - 1).trim(), format);
        validate();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Period) {
            Period fobj = (Period) obj;

            boolean lowerAreEqual = lowerInclusive == fobj.isLowerInclusive();
            boolean upperAreEqual = upperInclusive == fobj.isUpperInclusive();

            if (lower != null && fobj.getLower() != null) {
                lowerAreEqual = lowerAreEqual && lower.isEqual(fobj.getLower());
            } else {
                lowerAreEqual = lowerAreEqual && lower == fobj.getLower();
            }

            if (upper != null && fobj.getUpper() != null) {
                upperAreEqual = upperAreEqual && upper.isEqual(fobj.getUpper());
            }

            return lowerAreEqual && upperAreEqual;
        }

        return false;
    }

    @Override
    public int hashCode() {
        String value = getValue();
        return value != null ? value.hashCode() : 0;
    }

    public Duration duration() {
        return Duration.between(lower, upper);
    }

    public Period shift(Duration duration) throws SQLException {
        return new Period(lower.plus(duration), upper.plus(duration), lowerInclusive, upperInclusive);
    }

    public boolean contains(OffsetDateTime dateTime) {
        return (lower.isBefore(dateTime) && upper.isAfter(dateTime)) ||
                (lowerInclusive && lower.isEqual(dateTime)) ||
                (upperInclusive && upper.isEqual(dateTime));
    }

    public boolean overlap(Period period) {
        return contains(period.lower) || contains(period.upper);
    }

    private void validate() throws SQLException {
        if (lower == null || upper == null) {
            throw new SQLException("The lower and upper bounds must be defined.");
        }

        if (lower.isAfter(upper)) {
            throw new SQLException("The lower bound must be less than or equal to the upper bound.");
        }

        if (lower.isEqual(upper) && (!lowerInclusive || !upperInclusive)) {
            throw new SQLException("The lower and upper bounds must be inclusive for an instant period.");
        }
    }
}
