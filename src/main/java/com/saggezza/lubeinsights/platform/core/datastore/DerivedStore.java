package com.saggezza.lubeinsights.platform.core.datastore;

/**
 * Created by chiyao on 10/7/14.
 */

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.FieldAddress;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.common.kafka.KafkaConsumer;
import com.saggezza.lubeinsights.platform.core.common.kafka.KafkaUtil;
import com.saggezza.lubeinsights.platform.core.common.metadata.ZKUtil;
import com.saggezza.lubeinsights.platform.core.common.modules.ModuleFactory;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceConfig;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * DerivedStore is a data store derived from another data store. It can not be upserted from a data store client.
 * Instead, it will get input data steam from Kafka. Data elements will be aggregated in memory and then flushed into hbase.
 */
public class DerivedStore extends DataStore {

    public static final int LISTENING_TIMEOUT = 10; // seconds
    public static final Logger logger = Logger.getLogger(DerivedStore.class);
    public static final ServiceConfig config = ServiceConfig.load();
    protected String fromName;
    protected String temporalKey;
    protected String[] groupByKeys;
    protected String[] aggFields;
    protected String filterName;
    protected transient Predicate<DataElement> filter = null;
    protected transient AggregationCache aggCache;
    protected transient KafkaConsumer kafkaConsumer;
    protected transient boolean stopFlag = false;
    protected transient boolean onFlag = false;
    ExecutorService executor = null;


    public DerivedStore(String name, String fromName, StorageEngine storageEngine, String temporalKey, String[] groupByKeys, String[] aggFields, String filterName) {
        super(name,deriveDataModel(fromName, groupByKeys, aggFields),storageEngine,groupByKeys);
        this.fromName = fromName;
        this.temporalKey = temporalKey;
        this.groupByKeys = groupByKeys;
        this.aggFields = aggFields;
        this.filterName = filterName;
        if (filterName != null) {
            filter = (Predicate<DataElement>) ModuleFactory.getModule("predicate", filterName);
        }
    }

    /**
     * derive this data model
     * @param fromName
     * @param groupByKeys
     * @param aggFields
     * @return
     */
    protected static final DataModel deriveDataModel(String fromName, String[] groupByKeys, String[] aggFields) {
        DataModel dataModel = DataStoreCatalog.getDataStore(config.get("tenant"),config.get("application"),fromName).getDataModel();
        LinkedHashMap<String,DataModel> tm =  new LinkedHashMap<String,DataModel>();
        // build model for groupBy part
        for (int i=0; i<groupByKeys.length;i++) {
            tm.put(groupByKeys[i],dataModel.getField(new FieldAddress(groupByKeys[i])));
        }
        // build model for aggField part
        for (int i=0; i<aggFields.length;i++) {
            if (dataModel.getField(new FieldAddress(aggFields[i])).getDataType()==DataType.NUMBER) {
                tm.put(aggFields[i], DataModel.statsDataModel); // each agg field is a stats list (count,min,max,sum,sqsum)
            }
            else {
                tm.put(aggFields[i], DataModel.statsDataModelCountOnly); // TEXT and DATE only support count stats
            }
        }
        // add batchId into model
        tm.put("batchid",new DataModel(DataType.TEXT)); // batch is the scope of aggregation
        tm.put("flushid",new DataModel(DataType.NUMBER));
        // return the result data model
        return new DataModel(tm);
    }


    /**
     * start getting input data
     */
    public synchronized void start() {

        if (onFlag) {
            logger.info("DerivedStore "+name+" already started");
            return; // already started
        }

        stopFlag = false;
        // set up storage engine
        setupStorageEngine();

        // set up aggCache
        aggCache = new AggregationCache(name,dataModel,temporalKey,groupByKeys,aggFields);

        // set up kafka consumer
        kafkaConsumer = new KafkaConsumer(name,false);  // groupId = name, forBatch = false
        //kafkaConsumer = new KafkaConsumer(String.valueOf(System.currentTimeMillis()),true);
        logger.info("DerivedStore "+name+" created Kafka consumer to subscribe data");
        LinkedBlockingQueue<String> dataQueue = kafkaConsumer.start(getTopic());
        Runnable job = new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        String data = dataQueue.poll(LISTENING_TIMEOUT, TimeUnit.SECONDS);
                        if (data == null) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("No data from dataQueue");
                            }
                            if (stopFlag) { // only shut down when no more incoming data
                                break;
                            }
                            continue;
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug("From dataQueue: " + data);
                        }
                        System.out.println("From dataQueue: " + data);
                        if (data.equals(KafkaUtil.EOB)) {
                            logger.info("Found EOB, so commit Kafka consumer and flush aggregationCache");
                            aggCache.endOfBatch();
                            continue;
                        }
                        //decompose data into batchId and msg
                        int pos = data.indexOf('_');
                        String batchId = data.substring(0,pos);
                        String eltData = data.substring(pos+1);
                        DataElement elt = DataElement.fromString(eltData);
                        if (filter == null || filter.test(elt)) {
                            aggCache.addDataElement(batchId,transformElement(elt));
                        }
                    }
                    // got here only when stopFlag is true and no more new data
                    if (kafkaConsumer != null) {
                        kafkaConsumer.close();
                    }
                    else {
                        System.out.println("null kafkaConsumer");
                    }

                } catch (Exception e) {
                    logger.trace(e);
                    e.printStackTrace();
                }
            } // run
        }; // job
        if (executor == null) {
            executor = Executors.newFixedThreadPool(1);
        }
        executor.submit(job);
        onFlag = true;
        logger.info("DerivedStore "+name+" started");
    }

    /**
     * stop getting input data
     */
    public synchronized void stop() {
        if (!onFlag) {
            return; // already off
        }

        try {
            // shut down in data flow order (from upstream to downstream)
            stopFlag = true; // stop kafka consumer thread
            if (executor != null) {
                executor.shutdown();
                if (executor.awaitTermination(15, TimeUnit.SECONDS)) {
                    logger.info("Derived Store " + name + " executor tasks terminated");
                } else {
                    executor.shutdownNow();
                    logger.info("Timed out waiting. Forced Derived Store " + name + " executor to shutdown");
                }
                executor = null;
            }
            aggCache.stop();

        } catch (Exception e) {
            e.printStackTrace();
            logger.trace(e.getMessage(),e);
        }

    }

    private final String getTopic() {
        return StorageUtil.getTableName(config.get("tenant"),config.get("application"),fromName);
    }

    private DataElement transformElement(DataElement elt) {
        try {
            if (filter != null && !filter.test(elt)) {
                return null;
            }
            LinkedHashMap<String, DataElement> map = new LinkedHashMap<String, DataElement>();
            for (int i = 0; i < groupByKeys.length; i++) {
                map.put(groupByKeys[i], elt.valueByName(groupByKeys[i]));
            }
            for (int i = 0; i < aggFields.length; i++) {
                map.put(aggFields[i], elt.valueByName(aggFields[i]));
            }
            return new DataElement(map); // return the transformed element
        } catch (Exception e) {
            try {
                String msg = "bad element: " + elt.toString();
                e.printStackTrace();
                logger.trace(msg, e);
                System.out.println(msg);
            }catch (NullPointerException e1){
                e1.printStackTrace();
            }
            return null;
        }
    }


    public final void setupStorageEngine() throws RuntimeException {
        Connection conn = null;
        Statement stmt = null;
        try {
            String address = config.get("storage-engine").split(",")[1].trim();  // in config file:  storage-engine = hbase,localhost:2182
            // set up table
            String tableName = StorageUtil.getTableName(config.get("tenant"),config.get("application"),name);
            String sql = StorageUtil.genSqlCreateAggTable(tableName, groupByKeys, aggFields, dataModel);
            logger.info("DerivedStore "+name+" setupStorageEngine: "+ sql);
            conn = DriverManager.getConnection("jdbc:phoenix:" + address);
            stmt = conn.createStatement();
            stmt.executeUpdate(sql);
       } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new RuntimeException(e);
       } finally {
            if (stmt != null) {
                try {stmt.close();} catch (Exception e){}
            }
            if (conn != null) {
                try {conn.close();} catch (Exception e){}
            }
       }

    }


    public static final void main(String[] args) {
        try {
            DerivedStore store2 = (DerivedStore) DataStoreCatalog.getDataStore(config.get("tenant"), config.get("application"), "store02");
            System.out.println(store2.serialize());
            store2.start();
            Thread.sleep(2000);
            store2.stop();
            ZKUtil.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
