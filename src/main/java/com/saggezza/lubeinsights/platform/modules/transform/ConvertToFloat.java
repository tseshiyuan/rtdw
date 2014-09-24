package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;

import java.math.BigInteger;

/**
 * @author : Albin
 * Convert the input data into decimal.
 */
public class ConvertToFloat implements Function {

    public ConvertToFloat(){}

    private ConvertToFloat(Params params){
    }

    @Override
    public DataElement apply(DataElement val) {
        return new DataElement(DataType.NUMBER, new Double(val.asText())) ;
    }

    @Override
    public DataModel apply(DataModel in) {
        return new DataModel(DataType.NUMBER);
    }
}
