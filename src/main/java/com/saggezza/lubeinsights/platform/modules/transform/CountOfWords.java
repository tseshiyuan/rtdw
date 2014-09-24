package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;

/**
 * @author : Albin
 * Counts the words in the input separated by a space.
 */
public class CountOfWords implements Function {

    public CountOfWords(){}

    private CountOfWords(Params params){
    }

    @Override
    public DataElement apply(DataElement line) {
        return new DataElement(DataType.NUMBER, line.asText().split(" ").length);
    }

    @Override
    public DataModel apply(DataModel in) {
        return new DataModel(DataType.NUMBER);
    }

}
