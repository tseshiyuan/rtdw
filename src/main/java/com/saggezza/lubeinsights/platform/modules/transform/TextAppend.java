package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;

/**
 * @author : Albin
 * Appends/Prepends a specific string to data.
 */
public class TextAppend implements Function{

    private String textToAppend;
    private boolean atStart;

    public TextAppend(){}

    private TextAppend(Params params){
        this.textToAppend = params.getFirst();
        this.atStart = params.size() > 1 ? params.getSecond() : false;
    }

    @Override
    public DataElement apply(DataElement lineRec) {
        String line = lineRec.asText();
        String res = atStart ? (textToAppend + line) : (line + textToAppend);
        return new DataElement(DataType.TEXT, res);
    }

    @Override
    public DataModel apply(DataModel in) {
        return in;
    }
}
