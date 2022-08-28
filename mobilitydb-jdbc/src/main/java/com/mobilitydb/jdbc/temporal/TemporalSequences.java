package com.mobilitydb.jdbc.temporal;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;

public interface TemporalSequences<V extends Serializable> {
    /**
     * Gets the number of sequences
     * @return a number
     */
    int numSequences();

    /**
     * Gets the first sequence
     * @return a TSequence of type V
     */
    TSequence<V> startSequence();

    /**
     * Gets the last sequence
     * @return a TSequence of type V
     */
    TSequence<V> endSequence();

    /**
     * Gets the sequence located at the index position
     * @param n - the index
     * @return a TSequence of type V
     * @throws SQLException
     */
    TSequence<V> sequenceN(int n) throws SQLException;

    /**
     * Gets all sequences
     * @return a list of TSequence of type V
     */
    List<TSequence<V>> sequences();
}
