package com.saggezza.lubeinsights.platform.modules.spark;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRefType;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.DataExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.DataModelExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineMetaSupport;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineModule;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.SparkExecutionContext;
import com.saggezza.lubeinsights.platform.core.datastore.DataStoreClient;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceRequest;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceResponse;
import org.apache.spark.api.java.JavaRDD;

import java.util.*;
import java.util.Map;

/**
 * @author : Albin
 */
public class Write implements DataEngineModule, DataEngineMetaSupport{

    private final Map<String, Object> map;

    public Write(Params params) {
        //TODO - validate for pairs and throw right exceptions
        this.map = params.asMap();
    }

    @Override
    public void execute(ServiceRequest.ServiceStep step, DataExecutionContext context) throws DataEngineExecutionException {
        SparkExecutionContext sec = (SparkExecutionContext) context;
        DataChannel out = context.result() == null ? new DataChannel() : context.result().getData();
        for(Map.Entry<String, Object> entry : map.entrySet()){
            String tagName = entry.getKey();
            String dataStoreName = (String) entry.getValue();
            JavaRDD<DataElement> dataRef = sec.getDataRef(tagName);
            //Open client inside iteration. Some of the files may be empty.
            dataRef.foreachPartition((Iterator<DataElement> iter) -> {
                DataStoreClient dsClient = new DataStoreClient(dataStoreName);//TODO - remove this in outside spark, since this has to contact outside zookeeper
                dsClient.open(DataStoreClient.UPSERT_BATCH_MODE);
                while(iter.hasNext()){
                    DataElement elem = iter.next();
                    dsClient.upsert(elem);
                }
                dsClient.close();
            });

            out.putDataRef(tagName, new DataRef(DataRefType.STORE, dataStoreName));
            sec.setResponse(new ServiceResponse("OK", "OK", out));
        }
    }

    @Override
    public void mockExecute(ServiceRequest.ServiceStep step, DataExecutionContext context) throws DataEngineExecutionException {
        //Do nothing, since this is not changing the data model for any tags.
    }
}
