package com.saggezza.lubeinsights.platform.core.datastore;

/**
 * Created by chiyao on 9/18/14.
 */

import com.saggezza.lubeinsights.platform.core.common.GsonUtil;
import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.Utils;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.common.metadata.ZKUtil;
import com.saggezza.lubeinsights.platform.core.serviceutil.*;
import com.saggezza.lubeinsights.platform.core.workflowengine.WorkFlow;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class manages all the data stores
 */
public class DataStoreManager extends PlatformService {

    public static final Logger logger = Logger.getLogger(DataStoreManager.class);

    private static ServiceConfig config;
    private String tenantName;
    private String applicationName;
    private StorageEngine storageEngine;  // let's just use one for now, TODO: different data store has different storage engine
    private ConcurrentHashMap<String, DataStore> allDataStores; // for this tenant and application
    private ConcurrentHashMap<String, DerivedStore> allDerivedStores;

    static {
        config = ServiceConfig.load();
    }

    /**
     * load all data stores and derive specs
     */
    public final void init(ServiceConfig config) {

        String engineSpec = config.get("storage-engine");
        tenantName = config.get("tenant");
        applicationName = config.get("application");
        if (engineSpec != null) {
            String[] typeLocation = engineSpec.split(",");
            storageEngine = new StorageEngine(typeLocation[0].trim(), typeLocation[1].trim());
        }

        // load all data stores
        allDataStores = DataStoreCatalog.getDataStores(tenantName,applicationName);
        allDerivedStores = new ConcurrentHashMap<String,DerivedStore>();
        // track all derived stores
        for (Map.Entry<String,DataStore> kv: allDataStores.entrySet()) {
            if (kv.getValue() instanceof DerivedStore) {
                allDerivedStores.put(kv.getKey(),(DerivedStore)kv.getValue());
            }
        }
    }

    public DataStoreManager() {super(ServiceName.DATASTORE_MANAGER);}

    public final void start(int port) throws Exception {
        init(config);
        super.start(port);
    }

    public ServiceResponse processRequest(ServiceRequest request, String command) {
        try {

            logger.info("DataStoreManager processes request:\n" + request.toJson());

            if (request == null) {
                logger.error("request is null");
            }

            ArrayList<ServiceRequest.ServiceStep> steps = request.getCommandList();

            if (steps == null || steps.size() != 1) {
                return new ServiceResponse("ERROR", "Bad Request for DataStoreManager. Only one command is allowed in request", null);
            }
            ServiceRequest.ServiceStep step = steps.get(0);

            // command interpreter
            Params params = step.getParams();
            ServiceResponse response = null;
            switch (step.getCommand()) {
                case NEW_STORE:
                    DataStore newStore = newDataStore((String)params.getValue("name"),
                            (DataModel)params.getValue("dataModel"),
                            storageEngine,
                            (String[])params.getValue("indexFields"),
                            (Boolean)params.getValue("force"));
                    if (newStore == null) {
                        response = new ServiceResponse("OK", "Data store already exists", null);
                    }else{
                        response = new ServiceResponse("OK", "Data store created", null);
                    }
                    break;
                case NEW_DERIVED_STORE:
                    newDerivedStore((String)params.getValue("name"),
                            (String)params.getValue("forName"),
                            storageEngine,
                            params.getValue("temporalKey"),
                            (String[])params.getValue("groupByKeys"),
                            (String[])params.getValue("aggFields"),
                            (String)params.getValue("filterName"),
                            (Boolean)params.getValue("force"));
                    response = new ServiceResponse("OK",null,null);
                    break;
                case DELETE_STORE:
                    deleteDataStore((String)params.getValue("name"));
                    response = new ServiceResponse("OK",null,null);
                    break;
                case START_DERIVED_STORE:
                    String name = (String)params.getValue("name");
                    if (startDerivedStore(name)) {
                        response = new ServiceResponse("OK", null, null);
                    }
                    else {
                        response = new ServiceResponse("ERROR", "Cannot start derived store ", null);
                    }
                    break;
                case STOP_DERIVED_STORE:
                    name = (String)params.getValue("name");
                    if (startDerivedStore(name)) {
                        response = new ServiceResponse("OK", null, null);
                    }
                    else {
                        response = new ServiceResponse("ERROR", "Cannot stop derived store ", null);
                    }
                    break;
                case START_ALL_DERIVED_STORES:
                    startAllDerivedStores();
                    response = new ServiceResponse("OK", null, null);
                    break;
                case STOP_ALL_DERIVED_STORES:
                    stopAllDerivedStores();
                    response = new ServiceResponse("OK", null, null);
                    break;

                default:
                    response = new ServiceResponse("ERROR", "Bad command for DataStoreManager", null);
            }
            logger.info("DataStoreManager's response:\n" + response.toJson());
            return response;
        } catch (Exception e) {
            e.printStackTrace();
            logger.trace("DataStoreManager Error", e);
            return new ServiceResponse("ERROR", e.getMessage(), null);
        }

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
     * called from platform app
     * add a new data store to catalog
     * // TODO: create underlying representation in HBase ?
     * @param name
     * @param dataModel
     * @param storageEngine
     * @param force
     */
    public DataStore newDataStore(String name, DataModel dataModel, StorageEngine storageEngine, String[] indexFields, boolean force) {

        System.out.println("name = "+name);
        System.out.println("dataModel = " + dataModel.toString());
        System.out.println("indexFields = "+ GsonUtil.gson().toJson(indexFields));
        System.out.println("force="+force);

        if (allDataStores.containsKey(name) && !force) {
            logger.error("Data Store "+name+" already exits. Cannot add it.");
            return null;
        }
        else {
            DataStore store = new DataStore(name, dataModel, storageEngine, indexFields);
            // add it to catalog
            DataStoreCatalog.addDataStore(tenantName, applicationName, store);
            allDataStores.put(name, store);
            return store;
        }
    }

    /**
     * called from platform app
     * derive a new data store and add to catalog
     * @param name
     * @param fromName
     * @param storageEngine
     * @param temporalKey
     * @param groupByKeys
     * @param aggFields
     * @param filterName
     * @param force
     * @return
     */
    public DataStore newDerivedStore(String name, String fromName, StorageEngine storageEngine,
                                     String temporalKey, String[] groupByKeys, String[] aggFields, String filterName,
                                     boolean force) {
        DerivedStore store = new DerivedStore(name,fromName,storageEngine,temporalKey,groupByKeys,aggFields,filterName);
        // add it to catalog
        DataStoreCatalog.addDataStore(tenantName, applicationName, store);
        allDataStores.put(name, store);
        allDerivedStores.put(name,store);
        return store;
    }

    /**
     * remove a data store from catalog after closing it
     * This method does not really purge the data or schema in the underlying storage engine, however.
     * @param name
     */
    public final void deleteDataStore(String name) throws IOException {
        DataStore store = allDataStores.remove(name);
        DataStoreCatalog.removeDataStore(tenantName, applicationName, name);
    }


    /**
     * start a derived store if it exists
     * @param name
     * @return true if the derived store exists
     */
    public final boolean startDerivedStore(String name) {
        DerivedStore derivedStore = allDerivedStores.get(name);
        if (derivedStore != null) {
            derivedStore.start();
            return true;
        }
        else {
            return false;
        }
    }

    /**
     * stop a derived store if it exists
     * @param name
     * @return true if the derived store exists
     */
    public final boolean stopDerivedStore(String name) {
        DerivedStore derivedStore = allDerivedStores.get(name);
        if (derivedStore != null) {
            derivedStore.stop();
            return true;
        }
        else {
            return false;
        }
    }

    public void startAllDerivedStores() {
        for (DerivedStore d: allDerivedStores.values()) {
            try {
                d.start();
                logger.info("DataStoreManager starting DerivedStore "+d.name);
            } catch (Exception e) {
                logger.trace("Error starting DerivedStore "+d.name, e);
                e.printStackTrace();
            }
        }
    }

    public void stopAllDerivedStores() {
        for (DerivedStore d: allDerivedStores.values()) {
            try {
                d.stop();
                logger.info("Stopped DerivedStore " + d.name);
            } catch (Exception e) {
                logger.trace("Error stopping DerivedStore "+d.name, e);
                e.printStackTrace();
            }
        }
    }

    public static final void main(String[] args) {

        try {

            DataStoreManager mgr = new DataStoreManager();
            mgr.init(config); // load all data stores and derive specs

            // define data mode dm1: <userName:text, gender:text, age:number, createDate:number>
            LinkedHashMap<String, DataModel> tm1 = new LinkedHashMap<String, DataModel>();
            tm1.put("userName", new DataModel(DataType.TEXT));
            tm1.put("gender", new DataModel(DataType.TEXT));
            tm1.put("age", new DataModel(DataType.NUMBER));
            tm1.put("createDate", new DataModel((DataType.NUMBER)));
            DataModel dm1 = new DataModel(tm1);

            DataStore store01 = new DataStore("store01",dm1,mgr.storageEngine,new String[]{"userName","gender"}); // indexFields
            DataStoreCatalog.addDataStore(config.get("tenant"),config.get("application"),store01);

            DerivedStore store02 = new DerivedStore("store02","store01",mgr.storageEngine,
                    "createDate",   // windowName
                    new String[] {"createDate","gender"}, // groupByKeys
                    new String[] {"age"}, // aggFields
                    null // filterName
            );
            DataStoreCatalog.addDataStore(config.get("tenant"),config.get("application"),store02);

            // show them
            System.out.println(store01.serialize());
            System.out.println(store02.serialize());


            store02.setupStorageEngine();
/*
            // send elements to store1 (and propagate to store2)
            ArrayList<DataElement> al = readFromFile(args[0],store1.getDataModel());
            for (DataElement e: al) {
                store1.addDataElement(e);
            }
            // use empty as end of batch token
            //store1.addDataElement(DataElement.EMPTY);

            store1.close();
            store2.close();

            // temporary. will use PlatformService.stop later
            ZKUtil.close(); // TODO: why it takes so long?

            System.out.println("exit");
            System.exit(0);

*/
        } catch (Exception e) {e.printStackTrace();}

    }



}
