package com.saggezza.lubeinsights.platform.core.datastore;

/**
 * Created by chiyao on 9/18/14.
 */

import com.saggezza.lubeinsights.platform.core.common.GsonUtil;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.FieldAddress;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;

import java.io.IOException;
import java.util.HashMap;
import java.util.function.Function;

/**
 * It is a persistent table in hbase or any plugin storage engine with latest content cached
 * It supports real-time aggregation
 * New data arrives with a monotonic key (i.e. value increases in arrival order)
 * It is defined by a data model and a schema specifying how to represent the data model in storage engine.
 */
public class TemporalStore extends DataStore {

    // specify who derives from me (This is the serialized representation when persisted)
    private HashMap<String,DeriveSpec> derivers = null;
    // specify how to transform a data element to who (This is determined by derivers)
    private transient HashMap<TemporalStore, Function<DataElement,DataElement>> derivedStores = null;
    private transient CacheStore cacheStore = null;

    public TemporalStore(String name, DataModel dataModel, StorageEngine storageEngine, String[] indexNames) {
        super(name,dataModel,storageEngine, indexNames);
    }


    /**
     * This is a derived store, so we keep all the derived info here
     * @param windowName
     * @param groupByKeyAddress
     * @param aggFieldAddress
     */
    private void setDerivedInfo(String windowName, FieldAddress[] groupByKeyAddress, String[] aggFieldAddress) {
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
        // add newStore to my derivers
        if (derivers == null) {
            derivers = new HashMap<String,DeriveSpec>();
        }
        derivers.put(newStore.name, deriveSpec);
        // set up new store
        newStore.setDerivedInfo(
                deriveSpec.getDerivedTemporalKeyName(),
                deriveSpec.getDerivedGroupByKeyAddress(),
                deriveSpec.getAggFieldAlias());
        if (derivedStores == null) {
            derivedStores = new HashMap<TemporalStore, Function<DataElement,DataElement>>();
        }
        derivedStores.put(newStore, deriveSpec.getDataElementTransformer());
   }

    /**
     * add a data element to this, and also to all derived stores after transforming it
     * @param dataElement
     */
    public void addDataElement(DataElement dataElement) {
        System.out.println(name+ " receives "+ dataElement.toString());
        // add to my cache store
        if (cacheStore != null) {
            if (dataElement == DataElement.EMPTY) { // EOB, signal to flush
                cacheStore.flushRequest();
            }
            else {
                cacheStore.add(dataElement);
            }
        }
        // then add to all derived stores
        if (derivedStores != null) {
            for (TemporalStore store : derivedStores.keySet()) {
                // transform the element only if it's not empty
                DataElement newElt = (dataElement == DataElement.EMPTY ? dataElement : derivedStores.get(store).apply(dataElement));
                if (newElt != null) { // not filtered out
                    store.addDataElement(newElt);
                }
            }
        }
    }


    public final String toJson() {return GsonUtil.gson().toJson(this);} // TODO: make sure it serializes super

    public final void close()  throws IOException {
        if (cacheStore != null) {
            cacheStore.close();
        }
    }
}
