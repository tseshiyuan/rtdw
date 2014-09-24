package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;

/**
 * @author : Albin
 * Inserts a text into the data at the specified location.
 */
public class TextInsert implements Function {
    private Integer indexToInsert;
    private String textToInsert;

    public TextInsert(){}

    private TextInsert(Params params){
        indexToInsert = params.getFirst();
        textToInsert = params.getSecond();
    }

    @Override
    public DataElement apply(DataElement lineRec) {
        String line = lineRec.asText();
        String res = line.substring(0, indexToInsert) + textToInsert + line.substring(indexToInsert);
        return new DataElement(DataType.TEXT, res);
    }

    @Override
    public DataModel apply(DataModel in) {
        return in;
    }
}
