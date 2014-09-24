package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;
import org.apache.log4j.Logger;

/**
 * @author : Albin
 * Convert the data into uppercase.
 */
public class UpperCase implements Function {
    public UpperCase(){}

    private UpperCase(Params params){
    }

    @Override
    public DataElement apply(DataElement lineRec) {
        String line = lineRec.asText();
        String res = line.toUpperCase();
        return new DataElement(DataType.TEXT, res);
    }

    @Override
    public DataModel apply(DataModel in) {
        return in;
    }
}
