package com.saggezza.lubeinsights.platform.core.datastore;

/**
 * Created by chiyao on 9/5/14.
 */

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
public abstract class DataStore {

    public static final Logger logger = Logger.getLogger(DataStore.class);
    protected String name;
    protected DataModel dataModel;  // describes data model, storage paradigm (relational, columnar, key based memory
    protected StorageEngine storageEngine = null;
    protected String[] indexFields;
    protected transient KafkaConsumer kafkaConsumer = null;
    protected transient StorageEngineClient storageEngineClient = null;

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
    public void open(String topic) {
        setupStorageEngine();
        setupListener(topic);
    }

    public final void setupStorageEngine() {
        storageEngineClient = storageEngine.setupDataStore(this); // set up hbase table
    }

    public final void setupListener(String topic) {
        // set up kafka consumer
        kafkaConsumer = new KafkaConsumer(name,false); // groupId = name, forBatch = false
        LinkedBlockingQueue<String> queue = kafkaConsumer.start(topic);

        ExecutorService executor = Executors.newFixedThreadPool(1);

        Runnable job = new Runnable() {
            @Override
            public void run() {
                String data;
                DataElement dataElement;
                try {
                    while ((data = queue.take()) != null) { // blocking call
                        //System.out.println(data);
                        dataElement = DataElement.fromString(data);
                        storageEngineClient.aggsert(dataElement);
                        if (data.equalsIgnoreCase(KafkaUtil.EOB)) {
                            break;
                        }
                    }
                    kafkaConsumer.commit(); // TODO: handle consistency with HBase
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.trace(e);
                } finally {
                    if (kafkaConsumer != null) {
                        kafkaConsumer.close();
                        kafkaConsumer = null;
                    }
                    // close this storage engine client
                    if (storageEngineClient != null) {
                        try {
                            storageEngineClient.close();
                        } catch (Exception e) {}
                        storageEngineClient = null;
                    }
                }
            }
        };
        executor.submit(job);

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

    public abstract void close() throws IOException; // TODO

}
