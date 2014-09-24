package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;

/**
 * @author : Albin
 * Converts the input to lowercase.
 */
public class LowerCase implements Function {
    public LowerCase(){}

    public LowerCase(Params params){}

    @Override
    public DataElement apply(DataElement line) {
        return new DataElement(DataType.TEXT, line.asText().toLowerCase());
    }

    @Override
    public DataModel apply(DataModel in) {
        return in;
    }
}
