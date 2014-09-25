package com.saggezza.lubeinsights.platform.core.datastore;

/**
 * Created by chiyao on 9/5/14.
 */

import com.google.gson.Gson;
import com.saggezza.lubeinsights.platform.core.common.GsonUtil;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.*;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;

import java.util.concurrent.Future;

/**
 *
 * Data Store is a data container that supports insert, query, search operations
 * Unlike DataRef, its content is not frozen, and it can be backed by various storage engines.
 * Data Store can be used as a sink of data collection or data pipe, or as a the source for analytical queries
 */
public abstract class DataStore {

    protected String name;
    protected DataModel dataModel;  // describes data model, storage paradigm (relational, columnar, key based memory
    protected StorageEngine storageEngine = null;

    public DataStore(String name, DataModel dataModel, StorageEngine storageEngine) {
        this.name = name;
        this.dataModel = dataModel;
        this.storageEngine = storageEngine;
    }

    public final String getName() {return name;}

    public final DataModel getDataModel() {return dataModel;}

    public final StorageEngine getStorageEngine() {return storageEngine;}

    /**
     * add type tag and json notion together
     * @return
     */
    public final String serialize() {
        return this.getClass().getName()+ "__"+ GsonUtil.gson().toJson(this);
    }

    /**
     * for de-serialization (used by DataStore catalog)
     * @param serializedDataStore
     * @return
     */
    public static final String[] getTypeAndJson(String serializedDataStore) {
        return serializedDataStore.split("__");
    }

    public abstract void close();




}
