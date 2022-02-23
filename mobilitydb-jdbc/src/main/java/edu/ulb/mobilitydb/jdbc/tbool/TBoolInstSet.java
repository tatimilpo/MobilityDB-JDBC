package edu.ulb.mobilitydb.jdbc.tbool;

import edu.ulb.mobilitydb.jdbc.temporal.TInstantSet;

public class TBoolInstSet extends TInstantSet<Boolean, TBool> {

    public TBoolInstSet(TBool temporalDataType) throws Exception {
        super(temporalDataType);
    }

    public TBoolInstSet(String value) throws Exception {
        super(TBool::new, value);
    }

    public TBoolInstSet(String[] values) throws Exception {
        super(TBool::new, values);
    }

    public TBoolInstSet(TBoolInst[] values) throws Exception {
        super(TBool::new, values);
    }
}
