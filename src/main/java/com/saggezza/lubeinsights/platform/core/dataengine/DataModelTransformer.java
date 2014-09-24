package com.saggezza.lubeinsights.platform.core.dataengine;

import com.saggezza.lubeinsights.platform.core.common.modules.ModuleException;
import com.saggezza.lubeinsights.platform.core.common.modules.Modules;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceRequest;

/**
 * @author : Albin
 */
public class DataModelTransformer implements DataEngineExecutor {

    @Override
    public void execute(ServiceRequest.ServiceStep step, DataExecutionContext executionContext)
            throws DataEngineExecutionException, ModuleException {
        DataModelExecutionContext context = (DataModelExecutionContext) executionContext;
        Modules.metaModule("spark", step.getCommand().name(),
                step.getParams()).mockExecute(step, context);
    }

    @Override
    public String materializeOutput(String outConfig, DataExecutionContext in) {
        return null;
    }
}
