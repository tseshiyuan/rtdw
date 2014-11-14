package com.saggezza.lubeinsights.platform.core.datastore;

import com.saggezza.lubeinsights.platform.core.common.GsonUtil;
import com.saggezza.lubeinsights.platform.core.common.metadata.ZKUtil;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by chiyao on 9/24/14.
 */
public class DataStoreCatalog {

    public static final Logger logger = Logger.getLogger(DataStoreCatalog.class);

    public static final void addDataStore(String tenantName, String applicationName, DataStore dataStore) {
        try {
            String path = zkPath(tenantName, applicationName, dataStore.getName());
            ZKUtil.zkSet(path, dataStore.serialize());
        } catch (Exception e) {
            logger.error("Zookeeper access error: "+e.getMessage());
            throw new RuntimeException(e);
        }
    }


    public static final void removeDataStore(String tenantName, String applicationName, String name) {
        try {
            String path = zkPath(tenantName, applicationName, name);
            ZKUtil.zkRemove(path);
        } catch (Exception e) {
            logger.error("Zookeeper access error: "+e.getMessage());
            throw new RuntimeException(e);
        }
    }


    /**
     * look up zk nodes, get the type and json for the serialized data store, and convert to a DataStore object
     * @param tenantName
     * @param applicationName
     * @param name
     * @return the DataStore for name
     */
    public static final DataStore getDataStore(String tenantName, String applicationName, String name) {
        try {
            String path = zkPath(tenantName, applicationName, name);
            String serializedDataStore = ZKUtil.zkGet(path);
            String[] typeAndJson = DataStore.getTypeAndJson(serializedDataStore);
            return (DataStore) GsonUtil.gson().fromJson(typeAndJson[1], Class.forName(typeAndJson[0]));
        } catch (Exception e) {
            String msg = "Cannot get data store "+ name;
            logger.error(msg);
            throw new RuntimeException(msg,e);
        }
    }

    /**
     * return empty map if no data stores
     * @param tenantName
     * @param applicationName
     * @return
     */
    public static final ConcurrentHashMap<String, DataStore> getDataStores(String tenantName, String applicationName) {
        try {
            String path = zkPath(tenantName, applicationName);
            List<String> names = ZKUtil.getChildren(path);
            ConcurrentHashMap<String, DataStore> result = new ConcurrentHashMap<String, DataStore>();
            if (names != null) {
                for (String name : names) {
                    try {
                        result.put(name, getDataStore(tenantName, applicationName, name));
                    } catch (Exception e) {
                        logger.error("Cannot get data store "+name+ ". Continue with other data stores");
                    }
                }
            }
            return result;
        } catch (Exception e) {
            logger.error("Zookeeper access error: "+e.getMessage());
            throw new RuntimeException(e);
        }
    }


    public static final boolean hasDerivedStore(String tenantName, String applicationName, String name) {
        ConcurrentHashMap<String, DataStore> stores = getDataStores(tenantName, applicationName);
        for (Map.Entry<String,DataStore> e: stores.entrySet()) {
            DataStore store = e.getValue();
            if (store instanceof DerivedStore  && ((DerivedStore)store).fromName.equals(name)) {
                return true;
            }
        }
        return false;
    }

    private static final String zkPath(String tenantName, String applicationName, String dataStoreName) {
        return new StringBuilder("/").append(tenantName).append("/datastorecatalog/")
                .append(applicationName).append("/")
                .append(dataStoreName).toString();
    }

    private static final String zkPath(String tenantName, String applicationName) {
        return new StringBuilder("/").append(tenantName).append("/datastorecatalog/")
                .append(applicationName).toString();
    }

}
