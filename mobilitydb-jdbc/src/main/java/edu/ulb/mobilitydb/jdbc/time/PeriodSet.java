package edu.ulb.mobilitydb.jdbc.time;

import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@TypeName(name = "periodset")
public class PeriodSet extends DataType {
    private final List<Period> periodList;

    public PeriodSet() {
        super();
        periodList = new ArrayList<>();
    }

    public PeriodSet(String value) throws SQLException {
        this();
        setValue(value);
    }

    public PeriodSet(Period... periods) throws SQLException {
        this();
        Collections.addAll(periodList, periods);
        validate();
    }

    @Override
    public String getValue() {
        return String.format("{%s}", periodList.stream()
                .map(Period::toString)
                .collect(Collectors.joining(", ")));
    }

    @Override
    public void setValue(String value) throws SQLException {
        String trimmed = value.trim();

        if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
            Matcher m = Pattern.compile("[\\[|\\(].*?[^\\]\\)][\\]|\\)]")
                    .matcher(trimmed);
            while (m.find()) {
                periodList.add(new Period(m.group()));
            }
        } else {
            throw new SQLException("Could not parse period set value.");
        }

        validate();
    }

    public Period[] getPeriods() {
        return periodList.toArray(new Period[0]);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PeriodSet) {
            PeriodSet fobj = (PeriodSet) obj;

            return periodList.size() == fobj.periodList.size() &&
                    periodList.equals(fobj.periodList);
        }

        return false;
    }

    @Override
    public int hashCode() {
        String value = getValue();
        return value != null ? value.hashCode() : 0;
    }

    private void validate() throws SQLException {
        for (int i = 0; i < periodList.size(); i++) {
            Period x = periodList.get(i);

            if (periodIsInValid(x)) {
                throw new SQLException("All periods should have a value.");
            }

            if (i + 1 < periodList.size()) {
                Period y = periodList.get(i + 1);

                if (periodIsInValid(y)) {
                    throw new SQLException("All periods should have a value.");
                }

                if (x.getUpper().isAfter(y.getLower()) ||
                    (x.getUpper().isEqual(y.getLower()) && x.isUpperInclusive() && y.isLowerInclusive())) {
                    throw new SQLException("The periods of a period set cannot overlap.");
                }
            }
        }
    }

    private boolean periodIsInValid(Period period) {
        return period == null || period.getValue() == null;
    }
}
