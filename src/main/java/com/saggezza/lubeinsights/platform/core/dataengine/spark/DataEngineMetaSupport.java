package com.saggezza.lubeinsights.platform.core.dataengine.spark;

import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.DataExecutionContext;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceRequest;

import java.io.Serializable;

/**
 * @author : Albin
 */
public interface DataEngineMetaSupport extends Serializable{

    void mockExecute(ServiceRequest.ServiceStep step, DataExecutionContext context) throws DataEngineExecutionException;
}
