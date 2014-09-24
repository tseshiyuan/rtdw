package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;

/**
 * @author : Albin
 * Replaces data with a constant value.
 */
public class Constant implements Function{

    private Object constant;

    private Constant() {
    }

    private Constant(Params params){
        this.constant = params.getFirst();
    }


    @Override
    public DataElement apply(DataElement o) {
        return new DataElement(DataType.forValue(constant), constant);
    }

    @Override
    public DataModel apply(DataModel in) {
        return new DataModel(DataType.forValue(constant));
    }
}
