package com.saggezza.lubeinsights.platform.apps;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.datastore.DataStoreCatalog;
import com.saggezza.lubeinsights.platform.core.datastore.DataStoreClient;
import com.saggezza.lubeinsights.platform.core.datastore.StorageUtil;
import com.saggezza.lubeinsights.platform.core.serviceutil.*;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.TreeMap;

/**
 * Created by chiyao on 9/11/14.
 */

/**
 * This simple app works when datastore manager is running
 * It first starts all derived stores via data staore manager.
 * Then, reads input file and upserts all p[arsed elements to data store named "store01"
 * Then, stops all derived data stores.
 * The data elements upserted to store01 will show up in store02, which is a derived data store of store01.
 */
public class DataStoreApp {

    public static final Logger logger = Logger.getLogger(DataStoreApp.class);
    public static final ServiceConfig config = ServiceConfig.load();

    public static final void main(String[] args) {
        DataStoreClient client = null;
        ServiceGateway serviceGateway = null;
        try {
            serviceGateway = ServiceGateway.getServiceGateway();
            serviceGateway.sendRequest(ServiceName.DATASTORE_MANAGER, new ServiceRequest(ServiceCommand.START_ALL_DERIVED_STORES, null));
            client = new DataStoreClient("store01");
            client.open(DataStoreClient.UPSERT_BATCH_MODE);
            client.beginBatch("batch1");
            ArrayList<DataElement> al = StorageUtil.readFromFile(args[0], DataStoreCatalog.getDataStore(config.get("tenant"),config.get("application"),"store01").getDataModel());
            for (DataElement e: al) {
                client.upsert(e);
            }
            client.endBatch();
            Thread.sleep(5000);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                try {client.close();} catch (Exception e) {e.printStackTrace();}
            }
            if (serviceGateway != null) {
                try {
                    serviceGateway.sendRequest(ServiceName.DATASTORE_MANAGER, new ServiceRequest(ServiceCommand.STOP_ALL_DERIVED_STORES, null));
                } catch (Exception e) {e.printStackTrace();}
            }
        }
    }

}
