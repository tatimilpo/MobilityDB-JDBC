package com.mobilitydb.jdbc.tpoint;

import com.mobilitydb.jdbc.temporal.GetSingleTemporalValueFunction;
import com.mobilitydb.jdbc.temporal.TInstant;
import com.mobilitydb.jdbc.temporal.TInstantSet;
import com.mobilitydb.jdbc.tpoint.helpers.SRIDParseResponse;
import com.mobilitydb.jdbc.tpoint.helpers.SRIDParser;
import org.postgis.Point;

import java.sql.SQLException;

public abstract class TPointInstSet extends TInstantSet<Point> {
    private int srid;

    protected TPointInstSet(String value, GetSingleTemporalValueFunction<Point> getSingleTemporalValue)
            throws SQLException {
        super(value, getSingleTemporalValue, TPoint::compareValue);
        this.srid = SRIDParser.applySRID(srid, temporalValues);
    }

    protected TPointInstSet(int srid, String[] values, GetSingleTemporalValueFunction<Point> getSingleTemporalValue)
            throws SQLException {
        super(values, getSingleTemporalValue, TPoint::compareValue);
        this.srid = SRIDParser.applySRID(srid, temporalValues);
    }

    protected TPointInstSet(int srid, TInstant<Point>[] values) throws SQLException {
        super(values, TPoint::compareValue);
        this.srid = SRIDParser.applySRID(srid, temporalValues);
    }

    @Override
    protected String preprocessValue(String value) throws SQLException {
        String newString = super.preprocessValue(value);
        SRIDParseResponse response = SRIDParser.parseSRID(newString);
        srid = response.getSRID();
        return response.getParsedValue();
    }

    @Override
    public boolean equals(Object obj) {
        // It is not required to verify if the SRID are equals since it is applied to the temporal values
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        // It is not required to include the SRID since it is applied to the temporal values
        return super.hashCode();
    }
}
