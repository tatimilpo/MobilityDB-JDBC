package edu.ulb.mobilitydb.jdbc.time;

import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;

import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@TypeName(name = "timestampset")
public class TimestampSet extends DataType {
    private final List<OffsetDateTime> dateTimeList;
    private static final String FORMAT = "yyyy-MM-dd HH:mm:ssX";

    public TimestampSet() {
        super();
        dateTimeList = new ArrayList<>();
    }

    public TimestampSet(String value) throws SQLException {
        this();
        setValue(value);
    }

    @Override
    public String getValue() {
        DateTimeFormatter format = DateTimeFormatter.ofPattern(FORMAT);

        return String.format("{%s}", dateTimeList.stream()
                .map(format::format)
                .collect(Collectors.joining(", ")));
    }

    @Override
    public void setValue(String value) throws SQLException {
        String trimmed = value.trim();

        if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
            String[] values = trimmed.substring(1, trimmed.length() - 1).split(",");
            DateTimeFormatter format = DateTimeFormatter.ofPattern(FORMAT);

            for (String v : values) {
                OffsetDateTime date = OffsetDateTime.parse(v.trim(), format);
                dateTimeList.add(date);
            }
        } else {
            throw new SQLException("Could not parse timestamp set value.");
        }

        validate();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TimestampSet) {
            TimestampSet fobj = (TimestampSet) obj;

            if (dateTimeList.size() == fobj.dateTimeList.size()) {
                for (int i = 0; i < dateTimeList.size(); i++) {
                    if (!dateTimeList.get(i).isEqual(fobj.dateTimeList.get(i))) {
                        return false;
                    }
                }
            } else {
                return false;
            }

            return true;
        }

        return false;
    }

    @Override
    public int hashCode() {
        String value = getValue();
        return value != null ? value.hashCode() : 0;
    }

    private void validate() throws SQLException {
        for (int i = 0; i < dateTimeList.size(); i++) {
            OffsetDateTime x = dateTimeList.get(i);

            if (x == null) {
                throw new SQLException("All timestamps should have a value.");
            }

            if (i + 1 < dateTimeList.size()) {
                OffsetDateTime y = dateTimeList.get(i + 1);

                if (x.isAfter(y) || x.isEqual(y)) {
                    throw new SQLException("The timestamps of a timestamp set must be increasing.");
                }
            }
        }
    }
}
