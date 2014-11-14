package com.saggezza.lubeinsights.platform.core.dataengine;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceResponse;

import java.util.HashMap;
import java.util.Map;

/**
 * @author : Albin
 */
public class DataModelExecutionContext extends DataExecutionContext {

    private final Map<String, DataModel> dataRefs = new HashMap<>();
    private final DataModelTransformer transformer = new DataModelTransformer();
    private DataChannel dataChannel;

    @Override
    public DataChannel getDataChannel() {
        return dataChannel;
    }

    @Override
    public void setDataChannel(DataChannel dataChannel) {
        this.dataChannel = dataChannel;
    }

    public void copyTheSameModel(String from, String to){
        dataRefs.put(to, dataRefs.get(from));
    }

    @Override
    public DataModel getDataRef(String tagName) {
        return dataRefs.get(tagName);
    }

    @Override
    public void setDataRef(String name, Object dataRef) {
        dataRefs.put(name, (DataModel)dataRef);
    }

    @Override
    public DataModelTransformer executor() {
        return transformer;
    }

    @Override
    public void disconnect() {
        dataRefs.clear();
    }

    @Override
    public ServiceResponse result() {
        return new ServiceResponse("OK","OKAY", dataChannel);
    }

}
