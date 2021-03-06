package com.saggezza.lubeinsights.platform.core.datastore;

import com.google.common.primitives.UnsignedInteger;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.FieldAddress;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by chiyao on 9/18/14.
 */
public class CacheStore {

    public static final int MAX_RETENTION_WINDOWS=10; // max number of windows to keep before flushing
    public static final Logger logger = Logger.getLogger(CacheStore.class);

    private TemporalStore temporalStore;
    private ConcurrentHashMap<ByteArray,DataElement>[] windows = (ConcurrentHashMap<ByteArray,DataElement>[]) new ConcurrentHashMap[MAX_RETENTION_WINDOWS];
    private String windowName;
    private FieldAddress[] groupByKeyAddress;
    private String[] aggFieldAlias;
    private long maxWindowId = -1L; // the max window id of temporal key seen so far
    private Flusher flusher;
    private boolean flushRequest = false; // flusher has idled long, so request to flush


    public CacheStore(TemporalStore temporalStore, String windowName, FieldAddress[] groupByKeyAddress, String[] aggFieldAlias) {
        this.temporalStore = temporalStore;
        this.windowName = windowName;
        this.groupByKeyAddress = groupByKeyAddress;
        this.aggFieldAlias = aggFieldAlias;
    }

    public void close() {
        // flush all windows starting from the earliest one
        flush((int)((maxWindowId+1) % MAX_RETENTION_WINDOWS));  // this is async call
        //send a shutdown token to flusher
        flusher.send(new ConcurrentHashMap<ByteArray,DataElement>());  // empty map signaling shutdown
        flusher.close();
    }

    public final void flushRequest() {
        flushRequest = true;
    }
    /**
     * Add a new (aggregated or not) data element to this
     * If its temporal key is beyond the tracking window, flush all cached element in window
     * @param dataElement
     */
    public void add(DataElement dataElement) {
        long windowId = dataElement.valueByName(windowName).asNumber().longValue();
        // reject data element if it is too told
        if (windowId < maxWindowId - MAX_RETENTION_WINDOWS +1) {
            String msg = String.format("TemporalStore % discards too old a data element for window %, windowId=% maxWindowId=%",
                                        temporalStore.getName(),windowId,maxWindowId);
            logger.error(msg);
            return;
        }
        if (windowId > maxWindowId) { // TODO: default flushing policy can be customized with a gap threshold
            // flush all windows starting from the earliest one
            flush((int) (windowId % MAX_RETENTION_WINDOWS));  // this is async call
            maxWindowId = windowId;
        }
        else if (flushRequest) {
            // flush all windows starting from the earliest one
            flush((int)((maxWindowId+1) % MAX_RETENTION_WINDOWS));  // this is async call
        }
        // add to hash
        addAndAggregate(windowId,groupByKeyAddress,aggFieldAlias,dataElement);
    }



    /**
     * convert an array of primitive data elements into a single byte array key
     * @param elements
     * @return
     */
    private static final ByteArray generateKey(DataElement[] elements) {
        ByteBuffer bb = ByteBuffer.allocate(8*elements.length);
        for (DataElement element: elements) {
            switch (element.getTypeIfPrimitive()) {
                case TEXT:
                    UnsignedInteger symbolId = SymbolTable.getId(element.asText());
                    bb.put((byte)0).putInt(symbolId.intValue());
                    break;
                case NUMBER:
                    Number n = element.asNumber();
                    if (n instanceof Integer) {
                        bb.put((byte)1).putInt(((Integer)n).intValue());
                    }
                    else if (n instanceof Long) {
                        bb.put((byte)2).putLong(((Long)n).longValue());
                    }
                    else if (n instanceof Float) {
                        bb.put((byte)3).putFloat(((Float)n).floatValue());
                    }
                    else if (n instanceof Double) {
                        bb.put((byte)4).putDouble(((Double)n).doubleValue());
                    }
                    break;
                case DATETIME:
                    bb.put((byte)5).putLong(element.asDateTime().getTime());
                    break;
            }
        }
        return new ByteArray(bb.array());
    }



    /**
     * add dataElement to cache's corresponding group specified by groupByAddress,
     * and roll/add over all the field values specified in aggFieldAddress into the aggregated element
     * Each field includes 5 stats to add up: count, min, max, sum and square sum
     * @param windowId
     * @param aggFieldAlias
     * @param dataElement
     */
    public void addAndAggregate(long windowId, FieldAddress[] groupByAddress, String[] aggFieldAlias, DataElement dataElement) {
        int window = (int)(windowId % windows.length);
        if (windows[window]==null) {
            windows[window] = new ConcurrentHashMap<ByteArray,DataElement>();
        }
        // convert agg fields to FieldAddress
        FieldAddress[] aliasCoordinates = new FieldAddress[aggFieldAlias.length];
        for (int i=0; i<aggFieldAlias.length; i++) {
            aliasCoordinates[i] = new FieldAddress(aggFieldAlias[i]);
        }
        // get this element's aggFields' values
        DataElement[] aggFields = dataElement.getFields(aliasCoordinates);
        // get the groupBy values
        DataElement[] groupByValues = dataElement.getFields(groupByKeyAddress);

        // get the bucket (element) for this group
        ByteArray combinedGroupByKey = generateKey(groupByValues);
        DataElement elt = windows[window].get(combinedGroupByKey);

        // aggregate dataElement into the bucket (elt)
        if (aggFields[0] == DataElement.EMPTY) {   // there is no agg quantity, we simply track counts
            if (elt==null) { // 1st element of this group
                // set count = 1
                DataElement[] array = new DataElement[] {new DataElement(DataType.NUMBER,1.0f)};  // count=1
                elt = new DataElement(new TreeMap<String,DataElement>());
                elt.setValueNamed(aggFieldAlias[0],new DataElement(array));
                // init group by fields
                for (int i=0; i<groupByValues.length; i++) {
                    elt.setValueNamed((String)groupByKeyAddress[i].getCoordinate()[0] ,groupByValues[i]);
                }
                windows[window].put(combinedGroupByKey,elt);
            }
            else {  // increment count (in-place update)
                elt.valueByName(aggFieldAlias[0]).valueAt(0).addValue((float)1.0f);
            }
        }
        else if (elt==null) { // There is agg quantity, and this is the 1st element in this group. Set up the bucket.
            elt = dataElement.clone();
            // add 5 stats to each fields
            for (int i=0; i<aggFieldAlias.length; i++) {
                float value = aggFields[i].asNumber().floatValue();
                DataElement[] array = new DataElement[5];
                array[Stats.COUNT.ordinal()] = new DataElement(DataType.NUMBER,1.0f);  // count
                array[Stats.MIN.ordinal()] = new DataElement(DataType.NUMBER,value);   // min
                array[Stats.MAX.ordinal()] = new DataElement(DataType.NUMBER,value);   // max
                array[Stats.SUM.ordinal()] = new DataElement(DataType.NUMBER,value);   // sum
                array[Stats.SQSUM.ordinal()] = new DataElement(DataType.NUMBER,value*value);  // square sum
                elt.setValueNamed(aggFieldAlias[i],new DataElement(array)); // in-place update
            }
            // add new elt to hash
            windows[window].put(combinedGroupByKey,elt);
        }
        else { // there is agg quantity, and this is not the 1st, so add dataElement to bucket's stats
            DataElement[] oldAggFields = elt.getFields(aliasCoordinates);
            for (int i=0; i<oldAggFields.length; i++) {
                // in-place update
                ArrayList<DataElement> stats = oldAggFields[i].asList();
                float value = aggFields[i].asNumber().floatValue();
                stats.get(Stats.COUNT.ordinal()).addValue(1.0f);
                stats.get(Stats.MIN.ordinal()).addValue(value);
                stats.get(Stats.MAX.ordinal()).addValue(value);
                stats.get(Stats.SUM.ordinal()).addValue(value);
                stats.get(Stats.SQSUM.ordinal()).addValue(value*value);
            }
        }
    }

    /**
     * async call
     * flush all elements in all windows starting from startWindow
     * @param startWindow starting window index
     */
    public void flush(int startWindow) {
        if (flusher== null) {
            // convert groupByKeyAddress back to string
            flusher = new Flusher(temporalStore.getStorageEngine().getStorageEngineClient(
                                           temporalStore.getName(), getStorageKeys(), null, aggFieldAlias), // no regular field names
                                  this);
        }
        // move all windows to flush queue starting from startWindow
        for (int i=0; i<windows.length; i++) {
            int current = (i+startWindow) % MAX_RETENTION_WINDOWS;
            if (windows[current] != null) {
                System.out.println("flush window "+current);
                flusher.send(windows[current]);
                windows[current] = null;
            }
        }
    }


    /**
     * generate the rowkeys for HBase by combining windowName and groupBy
     * @return
     */
    public String[] getStorageKeys() {
        String[] keyNames = new String[groupByKeyAddress.length+1];
        keyNames[0] = windowName;
        for (int i=1; i<= groupByKeyAddress.length; i++) {
            keyNames[i] = groupByKeyAddress.toString();
        }
        return keyNames;
    }

}
