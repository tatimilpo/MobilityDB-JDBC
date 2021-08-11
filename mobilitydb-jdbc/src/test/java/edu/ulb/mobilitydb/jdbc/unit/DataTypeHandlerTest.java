package edu.ulb.mobilitydb.jdbc.unit;

import edu.ulb.mobilitydb.jdbc.DataTypeHandler;
import edu.ulb.mobilitydb.jdbc.boxes.STBox;
import edu.ulb.mobilitydb.jdbc.boxes.TBox;
import edu.ulb.mobilitydb.jdbc.time.Period;

import edu.ulb.mobilitydb.jdbc.time.PeriodSet;
import edu.ulb.mobilitydb.jdbc.time.TimestampSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;

import java.sql.SQLException;

import static org.mockito.Mockito.*;

class DataTypeHandlerTest {
    @Test
    @DisplayName("Verifying register types works")
    void testRegisterTypes() throws SQLException {
        PGConnection mockedConnection = mock(PGConnection.class);
        DataTypeHandler.INSTANCE.registerTypes(mockedConnection);
        verify(mockedConnection, atLeastOnce()).addDataType("period", Period.class);
        verify(mockedConnection, atLeastOnce()).addDataType("periodset", PeriodSet.class);
        verify(mockedConnection, atLeastOnce()).addDataType("timestampset", TimestampSet.class);
        verify(mockedConnection, atLeastOnce()).addDataType("tbox", TBox.class);
        verify(mockedConnection, atLeastOnce()).addDataType("stbox", STBox.class);
    }
}
