package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;

/**
 * @author : Albin
 * Searches and replaces a specific string with another.
 */
public class SearchAndReplace implements Function {

    private String toReplace;
    private String toSearch;

    public SearchAndReplace(){}

    private SearchAndReplace(Params params){
        this.toSearch = params.getFirst();
        this.toReplace = params.getSecond();
    }

    @Override
    public DataElement apply(DataElement line) {
        return new DataElement(DataType.TEXT, line.asText().replaceAll(toSearch, toReplace));
    }

    @Override
    public DataModel apply(DataModel in) {
        return in;
    }
}
