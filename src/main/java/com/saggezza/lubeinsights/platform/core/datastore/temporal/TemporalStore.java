package com.saggezza.lubeinsights.platform.core.datastore.temporal;

/**
 * Created by chiyao on 9/18/14.
 */

import com.saggezza.lubeinsights.platform.core.common.GsonUtil;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.datastore.DataStore;
import com.saggezza.lubeinsights.platform.core.datastore.StorageEngine;

import java.util.HashMap;
import java.util.function.Function;

/**
 * It is a persistent table in hbase or any plugin storage engine with latest content cached
 * It supports real-time aggregation
 * New data arrives with a monotonic key (i.e. value increases in arrival order)
 * It is defined by a data model and a schema specifying how to represent the data model in storage engine.
 */
public class TemporalStore extends DataStore {

    private HashMap<String,DeriveSpec> derivedInfo = null;
    // specify how to transform a data element to who
    private transient HashMap<TemporalStore, Function<DataElement,DataElement>> derivedStores = new HashMap<TemporalStore, Function<DataElement,DataElement>>();
    private transient CacheStore cacheStore;

    public TemporalStore(String name, DataModel dataModel, StorageEngine storageEngine) {
        super(name,dataModel,storageEngine);
    }


    private void setDerivedInfo(String windowName, Object[][] groupByKeyAddress, Object[][] aggFieldAddress) {
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

    /**
     * derive a new temporal store with the grouping/filtering deriveSpec
     * @param deriveSpec
     * @param newStore
     * @return
     */
    public synchronized final void derive(DeriveSpec deriveSpec, TemporalStore newStore) {
        // add newStore to my deriveInfo
        if (derivedInfo == null) {
            derivedInfo = new HashMap<String,DeriveSpec>();
        }
        derivedInfo.put(newStore.name, deriveSpec);
        // set up new store
        newStore.setDerivedInfo(
                deriveSpec.getDerivedTemporalKeyName(),
                deriveSpec.getDerivedGroupByKeyAddress(),
                deriveSpec.getDerivedAggFieldAddress());
        derivedStores.put(newStore, deriveSpec.getDataElementTransformer());
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


    public final String toJson() {return GsonUtil.gson().toJson(this);} // TODO: make sure it serializes super

    public final void close() {
        // TODO
    }
}
