package com.saggezza.lubeinsights.platform.core.dataengine;

import com.saggezza.lubeinsights.platform.core.common.modules.ModuleException;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceRequest;

/**
 * @author : Albin
 */
public interface DataEngineExecutor {
    void execute(ServiceRequest.ServiceStep step, DataExecutionContext executionContext) throws DataEngineExecutionException, ModuleException;

    String materializeOutput(String outConfig, DataExecutionContext in);
}
