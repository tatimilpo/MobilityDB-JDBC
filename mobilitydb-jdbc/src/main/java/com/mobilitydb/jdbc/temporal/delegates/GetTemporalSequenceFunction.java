package com.mobilitydb.jdbc.temporal.delegates;

import com.mobilitydb.jdbc.temporal.TSequence;

import java.io.Serializable;
import java.sql.SQLException;

public interface GetTemporalSequenceFunction<V extends Serializable>  {
    TSequence<V> run(String value) throws SQLException;
}
