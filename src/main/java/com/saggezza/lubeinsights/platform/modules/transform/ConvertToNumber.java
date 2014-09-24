package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;

/**
 * @author : Albin
 * Converts the input data into number.
 */
public class ConvertToNumber implements Function {

    public ConvertToNumber(){}

    private ConvertToNumber(Params params){
    }

    @Override
    public DataElement apply(DataElement line) {
        return new DataElement(DataType.NUMBER, Long.parseLong(line.asText()));
    }

    @Override
    public DataModel apply(DataModel in) {
        return new DataModel(DataType.NUMBER);
    }
}
