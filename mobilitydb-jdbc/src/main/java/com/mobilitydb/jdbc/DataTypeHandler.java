package com.mobilitydb.jdbc;

import com.mobilitydb.jdbc.tbool.TBool;
import com.mobilitydb.jdbc.tfloat.TFloat;
import com.mobilitydb.jdbc.time.PeriodSet;
import com.mobilitydb.jdbc.tint.TInt;
import com.mobilitydb.jdbc.tpoint.tgeog.TGeogPoint;
import com.mobilitydb.jdbc.tpoint.tgeom.TGeomPoint;
import com.mobilitydb.jdbc.boxes.STBox;
import com.mobilitydb.jdbc.boxes.TBox;
import com.mobilitydb.jdbc.core.DataType;
import com.mobilitydb.jdbc.core.TypeName;
import com.mobilitydb.jdbc.time.Period;
import com.mobilitydb.jdbc.time.TimestampSet;
import com.mobilitydb.jdbc.ttext.TText;
import org.postgresql.PGConnection;

import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Singleton helper used to register all the MobilityDB data types to the connection.
 */
public enum DataTypeHandler {
    INSTANCE;

    private final ArrayList<Class<? extends DataType>> types;

    DataTypeHandler() {
        types = new ArrayList<>();
        types.add(Period.class);
        types.add(PeriodSet.class);
        types.add(TimestampSet.class);
        types.add(TBox.class);
        types.add(STBox.class);
        types.add(TInt.class);
        types.add(TBool.class);
        types.add(TFloat.class);
        types.add(TText.class);
        types.add(TGeomPoint.class);
        types.add(TGeogPoint.class);
    }

    /**
     * Registers the type based on the TypeName annotation of the class
     * @param connection - the PGConnection
     * @throws SQLException if any of the classes does not implement PGobject
     */
    public void registerTypes(PGConnection connection) throws SQLException {
        for (Class<? extends DataType> clazz : types) {
            if (clazz.isAnnotationPresent(TypeName.class)) {
                connection.addDataType(clazz.getAnnotation(TypeName.class).name(), clazz);
            }
        }
    }
}
