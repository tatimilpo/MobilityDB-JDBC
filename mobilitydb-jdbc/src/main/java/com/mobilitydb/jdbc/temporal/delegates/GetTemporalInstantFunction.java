package com.mobilitydb.jdbc.temporal.delegates;

import com.mobilitydb.jdbc.temporal.TInstant;

import java.io.Serializable;
import java.sql.SQLException;

public interface GetTemporalInstantFunction<V extends Serializable> {
    TInstant<V> run(String value) throws SQLException;
}
