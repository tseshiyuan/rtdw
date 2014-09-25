package com.saggezza.lubeinsights.platform.core.datastore;

/**
 * Created by chiyao on 9/18/14.
 */

import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.datastore.temporal.DeriveSpec;
import com.saggezza.lubeinsights.platform.core.datastore.temporal.TemporalStore;
import com.saggezza.lubeinsights.platform.core.serviceutil.*;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * This class manages all the data stores
 */
public class DataStoreManager extends PlatformService {

    public static final Logger logger = Logger.getLogger(DataStoreManager.class);

    private static ServiceConfig config;
    private static String tenantName;
    private static String applicationName;
    private static StorageEngine storageEngine;  // let's just use one for now
    private static ConcurrentHashMap<String, DataStore> allDataStores;

    static {
        config = ServiceConfig.load();
        String engineSpec = config.get("storage-engine");
        tenantName = config.get("tenant");
        applicationName = config.get("application");
        if (engineSpec != null) {
            String[] typeLocation = engineSpec.split(",");
            storageEngine = new StorageEngine(typeLocation[0].trim(),typeLocation[1].trim());
        }
        // load all data stores
        allDataStores = DataStoreCatalog.getDataStores(tenantName,applicationName);
    }

    public DataStoreManager() {super(ServiceName.DATASTORE_MANAGER);}

    public ServiceResponse processRequest(ServiceRequest request) {
        return null;  // TODO
    }

    /**
     * get the data store for the name
     * @param name
     * @return
     */
    public final DataStore getDataStore(String name) {
        return allDataStores.get(name);
    }


    /**
     * add a new data store to catalog
     * // TODO: create underlying representation in HBase ?
     * @param name
     * @param dataModel
     * @param storageEngine
     * @param isTemporal
     */
    public void addDataStore(String name, DataModel dataModel, StorageEngine storageEngine, boolean isTemporal) {
        if (allDataStores.containsKey(name)) {
            logger.error("Data Store "+name+" already exits. Cannot add it.");
        }
        else {
            // create one and add to catalog
            DataStore store=null;
            if (isTemporal) {
                store = new TemporalStore(name, dataModel, storageEngine);
            }
            else {
                store = new ReferenceStore(name, dataModel, storageEngine);
            }
            DataStoreCatalog.addDataStore(tenantName, applicationName, store);
            allDataStores.put(name, store);
        }
    }

    /**
     * derive data store fromName into data store toName
     * @param fromName
     * @param toName
     */
    public void deriveDataStore(String fromName, String toName, DeriveSpec deriveSpec) {
        TemporalStore from = (TemporalStore) getDataStore(fromName);
        TemporalStore to = (TemporalStore) getDataStore(toName);
        from.derive(deriveSpec,to);
    }

    /**
     * remove a data store from catalog after closing it
     * This method does not really purge the data or schema in the underlying storage engine, however.
     * @param name
     */
    public final void removeDataStore(String name) {
        DataStore store = allDataStores.remove(name);
        store.close();
        DataStoreCatalog.removeDataStore(tenantName, applicationName, name);
    }





}
