package com.mobilitydb.jdbc.tpoint;

import com.mobilitydb.jdbc.temporal.GetSingleTemporalValueFunction;
import com.mobilitydb.jdbc.temporal.TSequence;
import com.mobilitydb.jdbc.temporal.TSequenceSet;
import com.mobilitydb.jdbc.temporal.TemporalValue;
import com.mobilitydb.jdbc.tpoint.helpers.SRIDParseResponse;
import com.mobilitydb.jdbc.tpoint.helpers.SRIDParser;
import org.postgis.Point;

import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

public class TPointSeqSet extends TSequenceSet<Point> {
    private int srid;

    protected TPointSeqSet(String value,
                           GetSingleTemporalValueFunction<Point> getSingleTemporalValue) throws SQLException {
        super(value, getSingleTemporalValue, TPoint::compareValue);
        applySRID();
    }

    protected TPointSeqSet(int srid,
                           boolean stepwise,
                           String[] values,
                           GetSingleTemporalValueFunction<Point> getSingleTemporalValue) throws SQLException {
        super(stepwise, values, getSingleTemporalValue, TPoint::compareValue);
        this.srid = srid;
        applySRID();
    }

    protected TPointSeqSet(int srid,
                           boolean stepwise,
                           TSequence<Point>[] values,
                           GetSingleTemporalValueFunction<Point> getSingleTemporalValue) throws SQLException {
        super(stepwise, values, getSingleTemporalValue, TPoint::compareValue);
        this.srid = srid;
        applySRID();
    }

    @Override
    protected String preprocessValue(String value) throws SQLException {
        String newString = super.preprocessValue(value);
        SRIDParseResponse response = SRIDParser.parseSRID(newString);
        srid = response.getSRID();
        return response.getParsedValue();
    }

    public void applySRID() throws SQLException {
        // TODO: Or maybe overload the applySRID in SRIDParser to support array of arrays?
        List<TemporalValue<Point>> flatValues = temporalValues.stream()
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
        srid = SRIDParser.applySRID(srid, flatValues);
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
