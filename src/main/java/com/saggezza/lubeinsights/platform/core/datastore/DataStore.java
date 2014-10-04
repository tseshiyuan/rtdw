package com.saggezza.lubeinsights.platform.core.datastore;

/**
 * Created by chiyao on 9/5/14.
 */

import com.google.gson.Gson;
import com.saggezza.lubeinsights.platform.core.common.GsonUtil;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.*;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
    protected String[] indexFields;

    public DataStore(String name, DataModel dataModel, StorageEngine storageEngine, String[] indexFields) {
        this.name = name;
        this.dataModel = dataModel;
        this.storageEngine = storageEngine;
        this.indexFields = indexFields;
    }

    public final String getName() {return name;}

    public final DataModel getDataModel() {return dataModel;}

    public final StorageEngine getStorageEngine() {return storageEngine;}

    public final String[] getIndexFields() {return indexFields;}

    /**
     * called when storage manager receives an open request
     */
    public void open() {
        setupStorageEngine();
        setupListener();
    }

    public final void setupStorageEngine() {
        storageEngine.setupDataStore(this); // set up hbase table
    }

    public final void setupListener() {
        // set up kafka consumer
    }

    /**
     * get all fields other than index fields
     * @return
     */
    public final String[] getRegularFields() {
        Collection<String> fields = dataModel.getAllFieldNames();
        for (String f: indexFields) {
            fields.remove(f);
        }
        return (String[]) fields.toArray();
    }

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

    public abstract void close() throws IOException;

}
