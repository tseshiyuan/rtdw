package com.saggezza.lubeinsights.platform.core.datastore;

import com.google.common.collect.ObjectArrays;
import com.google.common.primitives.UnsignedInteger;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.FieldAddress;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceConfig;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by chiyao on 10/7/14.
 */
public class AggregationCache {

    public static final int CAPACITY = 100000;
    public static final int MAX_AGE = 3600; // seconds
    public static final int TIMER_SLEEP_DURATION = 10; // seconds
    public static final Logger logger = Logger.getLogger(AggregationCache.class);
    public static final ServiceConfig config = ServiceConfig.load();
    public static final String storageEngineAddress = config.get("storage-engine").split(",")[1];  // in config file:  storage-engine = hbase,localhost:2182

    protected String dataStoreName;
    protected String temporalKey; // nullable
    protected String[] groupByKeys;
    protected String[] aggFields;
    protected boolean stopFlag = false;
    private HashMap<String,TreeMap<ByteArray,DataElement>> buckets = new HashMap<String,TreeMap<ByteArray,DataElement>>();
    private AtomicLong counter = new AtomicLong(0);
    private Object lock = new Object();
    private String upsertSql;


    public AggregationCache(String dataStoreName, DataModel dataModel, String temporalKey, String[] groupByKeys, String[] aggFields) {
        this.dataStoreName = dataStoreName;
        this.temporalKey = temporalKey;
        this.groupByKeys = groupByKeys;
        this.aggFields = aggFields;
        String tableName = StorageUtil.getTableName(config.get("tenant"), config.get("application"), dataStoreName);
        upsertSql = StorageUtil.genSqlUpsert(tableName, groupByKeys, aggFields, dataModel);
        startTimer();
    }

    public void addDataElement(String batchId, DataElement elt) {

        DataElement[] aggFieldValues = new DataElement[aggFields.length];
        for (int i=0; i<aggFields.length;i++) {
            aggFieldValues[i] = elt.valueByName(aggFields[i]);
        }
        DataElement[] groupByValues = new DataElement[groupByKeys.length];
        for (int i=0; i<groupByKeys.length;i++) {
            groupByValues[i] = elt.valueByName(groupByKeys[i]);
        }
        ByteArray groupKey = generateKey(groupByValues);
        synchronized (lock) {
            // create TreeMap for this batchId if not exists yet
            TreeMap<ByteArray,DataElement> bucket = buckets.get(batchId);
            if (bucket==null) {
                bucket = new TreeMap<ByteArray,DataElement>();
                buckets.put(batchId,bucket);
            }
            if (bucket.containsKey(groupKey)) { // add stats to this bucket
                DataElement oldElt = bucket.get(groupKey);
                for (int i = 0; i < aggFields.length; i++) {
                    // in-place update
                    ArrayList<DataElement> stats = oldElt.valueByName(aggFields[i]).asList();
                    stats.get(Stats.COUNT.ordinal()).addValue(1.0d);
                    if (aggFieldValues[i].isNumber()) { // only NUMBER support these stats
                        double value = aggFieldValues[i].asNumber().doubleValue();
                        stats.get(Stats.MIN.ordinal()).setIfSmaller(value);
                        stats.get(Stats.MAX.ordinal()).setIfLarger(value);
                        stats.get(Stats.SUM.ordinal()).addValue(value);
                        stats.get(Stats.SQSUM.ordinal()).addValue(value * value);
                    }
                }
            } else {   // new bucket, set up all 5 stats
                elt = elt.clone();
                DataElement[] array;
                for (int i = 0; i < aggFields.length; i++) {
                    if (aggFieldValues[i].isNumber()) {
                        double value = aggFieldValues[i].asNumber().doubleValue();
                        array = new DataElement[5];
                        array[Stats.COUNT.ordinal()] = new DataElement(DataType.NUMBER, 1.0d);  // count
                        array[Stats.MIN.ordinal()] = new DataElement(DataType.NUMBER, value);   // min
                        array[Stats.MAX.ordinal()] = new DataElement(DataType.NUMBER, value);   // max
                        array[Stats.SUM.ordinal()] = new DataElement(DataType.NUMBER, value);   // sum
                        array[Stats.SQSUM.ordinal()] = new DataElement(DataType.NUMBER, value * value);  // square sum
                    }
                    else {  // TEXT or DATE only support count stats (no min, max,sum,sqsum)
                        array = new DataElement[1];
                        array[0] = new DataElement(DataType.NUMBER, 1.0d);  // count
                    }
                    elt.setValueNamed(aggFields[i], new DataElement(array)); // in-place update
                }
                bucket.put(groupKey, elt);
            }
            counter.addAndGet(1);
        }
    }

    public void endOfBatch() {
        flush("EOB");
    }

    public void stop() {
        stopFlag = true;
        // TODO: wait for timer and flusher thread to complete
   }

    private static final ByteArray generateKey(DataElement[] elements) {
        ByteBuffer bb = ByteBuffer.allocate(8*elements.length);
        for (DataElement element: elements) {
            switch (element.getTypeIfPrimitive()) {
                case TEXT:
                    Integer symbolId = SymbolTable.getId(element.asText());
                    bb.put((byte)0).putInt(symbolId.intValue());
                    break;
                case NUMBER:
                    Number n = element.asNumber();
                    bb.put((byte)1).putDouble(((Double) n).doubleValue());
                    break;
                case DATETIME:
                    bb.put((byte)2).putLong(element.asDateTime().getTime());
                    break;
            }
        }
        return new ByteArray(bb.array());
    }

    /**
     * timer determines when to flush (by cache size and age) in case no endOfBatch received
     */
    private final void startTimer() {
        ExecutorService executor = Executors.newFixedThreadPool(1);
        Runnable job = new Runnable() {
            @Override
            public void run() {
                try {
                    int age = 0;
                    while (!stopFlag) {
                        int sleepTime = calculateSleepTime(age, counter.longValue());
                        if (sleepTime==0) {
                            flush("Timer");
                            age = 0;
                        }
                        else {
                            age += sleepTime;
                            Thread.sleep(sleepTime*1000);
                        }
                    }
                    flush("shutdown");
                    logger.info("Stopping timer for AggregationCache "+dataStoreName);
                    //System.out.println("Stopping timer for AggregationCache " + dataStoreName);
                } catch (Exception e) {
                    logger.trace(e);
                    e.printStackTrace();
                }
            } // run
        }; // job
        executor.submit(job);
    }

    /**
     * determine flush or sleep
     * @param age in minutes
     * @param recs  number of elements in buckets
     * @return number of minutes more to sleep, or 0 if need to flush
     */
    private final int calculateSleepTime(int age, long recs) {
        if (recs >= CAPACITY || age > MAX_AGE) {
            return 0;
        }
        if (recs < CAPACITY/2) {
            return TIMER_SLEEP_DURATION; // 60 seconds
        }
        else {
            return TIMER_SLEEP_DURATION/2; // 30 seconds
        }
    }

    /**
     * flush buckets asynchronously
     */
    private void flush(String triggeredBy) {

        final long flushId;
        final HashMap<String,TreeMap<ByteArray, DataElement>> flushBuckets;
        synchronized (lock) {
            if (buckets.isEmpty()) {
                logger.info("No data element to flush for "+dataStoreName+" (triggered by "+triggeredBy+")");
                return;
            }
            else {
                flushBuckets = buckets;
                buckets = new HashMap<String,TreeMap<ByteArray, DataElement>>();
                flushId = System.currentTimeMillis()/1000;
                logger.info("Flushing AggregationCache "+dataStoreName+ ", flushId "+flushId+" (triggered by "+triggeredBy+")");
            }
        }

        ExecutorService executor = Executors.newFixedThreadPool(1);
        Runnable job = new Runnable() {
            @Override
            public void run() {

                Connection conn = null;
                PreparedStatement pstmt = null;
                try {
                    //Properties prop = new Properties();
                    //prop.setProperty("UpsertBatchSize","1");  // TODO
                    conn = DriverManager.getConnection("jdbc:phoenix:" + storageEngineAddress);
                    pstmt = conn.prepareStatement(upsertSql);
                    //System.out.println(upsertSql);
                    for (String batchId: flushBuckets.keySet()) {
                        for (Map.Entry<ByteArray, DataElement> entry : flushBuckets.get(batchId).entrySet()) {
                            DataElement elt = entry.getValue();
                            if (logger.isDebugEnabled()) {
                                logger.debug(elt.toString());
                            }
                            DataElement value;
                            // set values for all fields
                            for (int i = 0; i < groupByKeys.length; i++) {
                                value = elt.valueByName(groupByKeys[i]);
                                System.out.println("groupByKey "+value);
                                if (value.isNumber()) {
                                    pstmt.setDouble(i + 1, ((Double) value.asNumber()).doubleValue());
                                } else {
                                    pstmt.setString(i + 1, value.asText());
                                }
                            }
                            int numFields = groupByKeys.length;
                            for (int i = 0; i < aggFields.length; i++) {
                                DataElement field = elt.valueByName(aggFields[i]);
                                value = field.valueAt(Stats.COUNT.ordinal());
                                pstmt.setDouble(++numFields, ((Double) value.asNumber()).doubleValue());
                                if (field.asList().size() > 1) { // TEXT and DATE don't support these stats
                                    value = field.valueAt(Stats.MIN.ordinal());
                                    pstmt.setDouble(++numFields, ((Double) value.asNumber()).doubleValue());
                                    value = field.valueAt(Stats.MAX.ordinal());
                                    pstmt.setDouble(++numFields, ((Double) value.asNumber()).doubleValue());
                                    value = field.valueAt(Stats.SUM.ordinal());
                                    pstmt.setDouble(++numFields, ((Double) value.asNumber()).doubleValue());
                                    value = field.valueAt(Stats.SQSUM.ordinal());
                                    pstmt.setDouble(++numFields, ((Double) value.asNumber()).doubleValue());
                                }
                            }
                            // set batchId and flushId
                            pstmt.setString(++numFields, batchId);
                            pstmt.setDouble(++numFields, (double) flushId);
                            pstmt.execute();
                            conn.commit();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (pstmt != null) {
                        try {pstmt.close();} catch (Exception e){}
                    }
                    if (conn != null) {
                        try {conn.close();} catch (Exception e){}
                    }

                }
           } // run
        }; // job
        executor.submit(job);
    } // flush

}
