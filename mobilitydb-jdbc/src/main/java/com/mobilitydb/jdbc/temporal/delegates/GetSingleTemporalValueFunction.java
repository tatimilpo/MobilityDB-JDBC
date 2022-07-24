package com.mobilitydb.jdbc.temporal.delegates;

import com.mobilitydb.jdbc.temporal.TemporalValue;

import java.io.Serializable;
import java.sql.SQLException;

public interface GetSingleTemporalValueFunction<V extends Serializable> {
    TemporalValue<V> run(String value) throws SQLException;
}
