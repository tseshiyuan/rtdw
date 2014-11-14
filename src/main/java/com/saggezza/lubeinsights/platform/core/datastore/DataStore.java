package com.saggezza.lubeinsights.platform.core.datastore;

/**
 * Created by chiyao on 9/5/14.
 */

import com.google.common.collect.ObjectArrays;
import com.google.gson.Gson;
import com.saggezza.lubeinsights.platform.core.common.GsonUtil;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.*;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.kafka.KafkaConsumer;
import com.saggezza.lubeinsights.platform.core.common.kafka.KafkaUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

/**
 *
 * Data Store is a data container that supports insert, query, search operations
 * Unlike DataRef, its content is not frozen, and it can be backed by various storage engines.
 * Data Store can be used as a sink of data collection or data pipe, or as a the source for analytical queries
 */
public class DataStore {

    public static final Logger logger = Logger.getLogger(DataStore.class);
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
     * get all fields other than index fields
     * @return
     */
    public final String[] getRegularFields() {
        Collection<String> fields = dataModel.getAllFieldNames();
        for (String f: indexFields) {
            fields.remove(f);
        }
        return (String[]) fields.toArray(new String[0]);
    }

    public final String[] getFields() {
        Collection<String> fields = dataModel.getAllFieldNames();
        return fields.toArray(new String[0]);
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

}
