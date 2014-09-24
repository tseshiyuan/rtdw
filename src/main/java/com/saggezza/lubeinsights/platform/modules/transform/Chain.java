package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.modules.Modules;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;

/**
 * @author : Albin
 *
 * Chains two functions as a compoung function
 */
public class Chain implements Function{

    private Function one;
    private Function two;

    private Chain(){}

    public Chain(Params params){
        String one = params.get(0);
        Params oneParams = params.get(1);
        String two = params.get(2);
        Params twoParams = params.get(3);
        this.one = Modules.function(one, oneParams);
        this.two = Modules.function(two, twoParams);
    }

    @Override
    public DataElement apply(DataElement in) {
        return two.apply(one.apply(in));
    }

    @Override
    public DataModel apply(DataModel in) throws DataEngineExecutionException {
        return two.apply(one.apply(in));
    }

}
