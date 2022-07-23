package com.mobilitydb.jdbc.tpoint;

import com.mobilitydb.jdbc.temporal.GetSingleTemporalValueFunction;
import com.mobilitydb.jdbc.temporal.TInstant;
import com.mobilitydb.jdbc.temporal.TSequence;
import com.mobilitydb.jdbc.tpoint.helpers.SRIDParseResponse;
import com.mobilitydb.jdbc.tpoint.helpers.SRIDParser;
import org.postgis.Point;

import java.sql.SQLException;

public class TPointSeq extends TSequence<Point> {
    private int srid;

    protected TPointSeq(String value, GetSingleTemporalValueFunction<Point> getSingleTemporalValue)
            throws SQLException {
        super(value, getSingleTemporalValue, TPoint::compareValue);
        this.srid = SRIDParser.applySRID(srid, temporalValues);
    }

    protected TPointSeq(int srid, boolean stepwise, String[] values,
                        GetSingleTemporalValueFunction<Point> getSingleTemporalValue) throws SQLException {
        super(stepwise, values, getSingleTemporalValue, TPoint::compareValue);
        this.srid = SRIDParser.applySRID(srid, temporalValues);
    }

    protected TPointSeq(int srid, boolean stepwise, String[] values, boolean lowerInclusive, boolean upperInclusive,
                        GetSingleTemporalValueFunction<Point> getSingleTemporalValue) throws SQLException {
        super(stepwise, values, lowerInclusive, upperInclusive, getSingleTemporalValue, TPoint::compareValue);
        this.srid = SRIDParser.applySRID(srid, temporalValues);
    }

    protected TPointSeq(int srid, boolean stepwise, TInstant<Point>[] values) throws SQLException {
        super(stepwise, values, TPoint::compareValue);
        this.srid = SRIDParser.applySRID(srid, temporalValues);
    }

    protected TPointSeq(int srid, boolean stepwise, TInstant<Point>[] values,
                        boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(stepwise, values, lowerInclusive, upperInclusive, TPoint::compareValue);
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
