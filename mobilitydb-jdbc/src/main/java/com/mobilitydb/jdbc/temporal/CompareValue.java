package com.mobilitydb.jdbc.temporal;

import java.io.Serializable;

public interface CompareValue<V extends Serializable> extends Serializable {
    int run(V first, V second);
}
