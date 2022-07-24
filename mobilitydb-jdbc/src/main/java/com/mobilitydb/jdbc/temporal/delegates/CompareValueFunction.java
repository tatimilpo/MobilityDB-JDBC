package com.mobilitydb.jdbc.temporal.delegates;

import java.io.Serializable;

public interface CompareValueFunction<V extends Serializable> extends Serializable {
    int run(V first, V second);
}
