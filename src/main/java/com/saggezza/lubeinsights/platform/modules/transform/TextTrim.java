package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;

/**
 * @author : Albin
 * Trims any whitespace on boths ends in the data.
 */
public class TextTrim implements Function {
    public TextTrim(){}

    private TextTrim(Params params){
    }

    @Override
    public DataElement apply(DataElement elem) {
        String s= elem.asText();
        String res = null == s ? null : s.trim();
        return new DataElement(DataType.TEXT, res);
    }

    @Override
    public DataModel apply(DataModel in) {
        return in;
    }
}
