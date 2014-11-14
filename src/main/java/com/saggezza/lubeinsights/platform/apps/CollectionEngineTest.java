package com.saggezza.lubeinsights.platform.apps;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRefType;
import com.saggezza.lubeinsights.platform.core.serviceutil.*;
import org.apache.log4j.Logger;

/**
 * Created by chiyao on 9/11/14.
 */
public class CollectionEngineTest {

    public static final Logger logger = Logger.getLogger(CollectionEngineTest.class);

    public static final void main(String[] args) {
        try {

            Params params = Params.ofPairs(
                    "sourceDesc", "FileCollector.myCollector",
                    "dataModel", null,
                    "dataRef", new DataRef(DataRefType.FILE, args[0]), // specify destination as program argument
                    "batchId", "activity-log",
                    "parser", null,
                    "cleanup", Boolean.FALSE
            );
            ServiceRequest request = new ServiceRequest(ServiceCommand.COLLECT_BATCH, params);
            ServiceGateway gateway = ServiceGateway.getServiceGateway();
            ServiceResponse response = gateway.sendRequest(ServiceName.COLLECTION_ENGINE, request);

            System.out.println("status: " + response.getStatus());
            System.out.println("message: " + response.getMessage());
            System.out.println("data: " + response.getData());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
