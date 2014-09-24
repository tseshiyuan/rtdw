package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;

/**
 * @author : Albin
 * Finds the length of the data.
 */
public class TextLength implements Function{
    public TextLength(){}

    private TextLength(Params params){
    }

    @Override
    public DataElement apply(DataElement elem) {
        String s = elem.asText();
        int length = s == null ? 0 : s.length();
        return new DataElement(DataType.NUMBER, length);
    }

    @Override
    public DataModel apply(DataModel in) {
        return new DataModel(DataType.NUMBER);
    }

}
