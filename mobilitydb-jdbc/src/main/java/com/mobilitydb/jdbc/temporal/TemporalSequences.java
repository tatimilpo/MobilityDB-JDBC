package com.mobilitydb.jdbc.temporal;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;

public interface TemporalSequences<V extends Serializable> {
    int numSequences();
    TSequence<V> startSequence();
    TSequence<V> endSequence();
    TSequence<V> sequenceN(int n) throws SQLException;
    List<TSequence<V>> sequences();
}
