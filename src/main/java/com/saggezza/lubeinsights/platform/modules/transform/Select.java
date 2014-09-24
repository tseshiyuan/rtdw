package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.Selection;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;

/**
 * @author : Albin
 * Selects a subset of the available data elements.
 */
public class Select implements Function {

    private Selection selection;

    public Select(){}

    private Select(Params params){
        this.selection = params.getFirst();
    }


    @Override
    public DataElement apply(DataElement record) {
        return record.select(selection);
    }

    @Override
    public DataModel apply(DataModel in) throws DataEngineExecutionException {
        return in.select(selection);
    }

}
