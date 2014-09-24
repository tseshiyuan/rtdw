package com.saggezza.lubeinsights.platform.core.dataengine;

import com.saggezza.lubeinsights.platform.core.common.Environment;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceResponse;

import java.util.Objects;

/**
 * @author : Albin
 *
 * Execution context gives data engine access to the underlying system like Spark
 */
public abstract class DataExecutionContext {

    public abstract DataChannel getDataChannel();

    public abstract void setDataChannel(DataChannel dataChannel);

    public abstract Object getDataRef(String tagName);

    public abstract void setDataRef(String name, Object dataRef);

    public abstract DataEngineExecutor executor();

    public abstract void disconnect();

    public abstract ServiceResponse result();


}
