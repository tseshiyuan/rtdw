package com.saggezza.lubeinsights.platform.core.datastore;

/**
 * Created by chiyao on 9/18/14.
 */

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceConfig;

import java.util.HashMap;
import java.util.function.Function;

/**
 * It is a persistent table in hbase or any plugin storage engine with latest content cached
 * It supports real-time aggregation
 * New data arrives with a monotonic key (i.e. value increases in arrival order)
 * It is defined by a data model and a schema specifying how to represent the data model in storage engine.
 */
public class TemporalStore {

    private static ServiceConfig config;
    private static StorageEngine storageEngine = null;

    private String name;
    // specify how to transform a data element to who
    private HashMap<TemporalStore, Function<DataElement,DataElement>> derivedStores = new HashMap<TemporalStore, Function<DataElement,DataElement>>();
    private CacheStore cacheStore;

    static {
        config = ServiceConfig.load();
        String engineSpec = config.get("storage-engine");
        if (engineSpec != null) {
            String[] typeLocation = engineSpec.split(",");
            storageEngine = new StorageEngine(typeLocation[0].trim(),typeLocation[1].trim());
        }
    }

    private TemporalStore(String name, String windowName, Object[][] groupByKeyAddress, Object[][] aggFieldAddress) {
        this.name = name;
        // if no temporal key, then don't need a cache for aggregation
        if (windowName != null) {
            cacheStore = new CacheStore(this, windowName, groupByKeyAddress, aggFieldAddress);
        }
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object store) {
        return (store instanceof TemporalStore) && name.equals(((TemporalStore)store).name);
    }

    public final String getName() {return name;}

    /**
     * derive a new temporal store with the grouping/filtering spec
     * @param name
     * @param deriveSpec
     * @return
     */
    public final TemporalStore derive(String name, DeriveSpec deriveSpec) {
        // TODO: this need to be in persistent config (we can do it via DataStoreManager)
        TemporalStore newStore = new TemporalStore(name,
                deriveSpec.getDerivedTemporalKeyName(),
                deriveSpec.getDerivedGroupByKeyAddress(),
                deriveSpec.getDerivedAggFieldAddress());
        derivedStores.put(newStore, deriveSpec.getDataElementTransformer());
        return newStore;
    }


    /**
     * add a data element to this, and also to all derived stores after transforming it
     * @param dataElement
     */
    public void addDataElement(DataElement dataElement) {
        // add to my cache store
        if (cacheStore != null) {
            cacheStore.add(dataElement);
        }
        // then add to all derived stores
        for (TemporalStore store: derivedStores.keySet()) {
            store.addDataElement(derivedStores.get(store).apply(dataElement));
        }
    }

    /**
     * @return storage engine of this temporal store
     */
    public final StorageEngine getStorageEngine() {return storageEngine;}


}
