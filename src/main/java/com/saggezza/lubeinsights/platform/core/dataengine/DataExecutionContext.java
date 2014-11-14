package com.saggezza.lubeinsights.platform.core.dataengine;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRefType;
import com.saggezza.lubeinsights.platform.core.datastore.DataStoreClient;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceResponse;

import java.io.IOException;

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

    public DataRef loadStore(String eachTag, DataRef dataRef) throws IOException {
        if(dataRef.getType() != DataRefType.STORE){
            throw new RuntimeException("Data Reference should be of type store to be loaded as store "+dataRef.getType());
        }
        DataStoreClient client = new DataStoreClient(dataRef.getValue());
        client.open(DataStoreClient.QUERY_MODE);
        //TODO - Figure out which data ref type to use here, whether it is store of file. If file which FS to use. file:/// or hdfs://
        DataRef query = client.query(DataRefType.FILE, null, (String)null);
        client.close();
        return query;
    }
}
