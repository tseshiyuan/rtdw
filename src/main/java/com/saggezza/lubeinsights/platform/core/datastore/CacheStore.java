package com.saggezza.lubeinsights.platform.core.datastore;

import com.google.common.primitives.UnsignedInteger;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
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
    private Object[][] groupByKeyAddress;
    private Object[][] aggFieldAddress;
    private long maxWindowId = -1L; // the max window id of temporal key seen so far
    private Flusher flusher;
    private boolean flusherRequest = false; // flusher has idled long, so request to flush


    public CacheStore(TemporalStore temporalStore, String windowName, Object[][] groupByKeyAddress, Object[][] aggFieldAddress) {
        this.temporalStore = temporalStore;
        this.windowName = windowName;
        this.groupByKeyAddress = groupByKeyAddress;
        this.aggFieldAddress = aggFieldAddress;
    }

    public final void flusherRequest() {
        flusherRequest = true;
    }
    /**
     * Add a new (aggregated or not) data element to this
     * If its temporal key is beyond the tracking window, flush all cached element in window
     * @param dataElement
     */
    public void add(DataElement dataElement) {
        long windowId = dataElement.get(windowName).asNumber().longValue();
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
        else if (flusherRequest) {
            // flush all windows starting from the earliest one
            flush((int)((maxWindowId+1) % MAX_RETENTION_WINDOWS));  // this is async call
        }
        // add to hash
        ByteArray groupByKey = generateKey(dataElement.getElements(groupByKeyAddress));
        addAndAggregate(windowId,groupByKey,aggFieldAddress,dataElement);
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
     * add dataElement to cache with combined key windowId-groupByKey, and roll/add over all the fields specified in aggFieldAddress
     * Each field include 5 stats to add up: count, min, max, sum and square sum
     * @param windowId
     * @param groupByKey
     * @param aggFieldAddress
     * @param dataElement
     */
    public void addAndAggregate(long windowId, ByteArray groupByKey, Object[][] aggFieldAddress, DataElement dataElement) {
        int window = (int)(windowId % windows.length);
        DataElement elt = windows[window].get(groupByKey);
        if (elt==null) { // set up stats fields for new element
            elt = dataElement.clone();
            DataElement[] aggFields = elt.getElements(aggFieldAddress);
            // add stats to each fields
            for (DataElement field: aggFields) {
                float value = field.asNumber().floatValue();
                DataElement[] array = new DataElement[5];
                array[Stats.COUNT.ordinal()] = new DataElement(DataType.NUMBER,new DataElement(DataType.NUMBER,1));     // count
                array[Stats.MIN.ordinal()] = new DataElement(DataType.NUMBER,new DataElement(DataType.NUMBER,value));   // min
                array[Stats.MAX.ordinal()] = new DataElement(DataType.NUMBER,new DataElement(DataType.NUMBER,value));   // max
                array[Stats.SUM.ordinal()] = new DataElement(DataType.NUMBER,new DataElement(DataType.NUMBER,value));   // sum
                array[Stats.SQSUM.ordinal()] = new DataElement(DataType.NUMBER,new DataElement(DataType.NUMBER,value*value));  // square sum
                field.setToList(array); // in-place update
            }
            // add new elt to hash
            windows[window].put(groupByKey,elt);
        }
        else { // add stats to elt in hash
            DataElement[] aggFields = elt.getElements(aggFieldAddress);
            DataElement[] newEltFields = dataElement.getElements(aggFieldAddress);
            for (int i=0; i<aggFields.length; i++) {
                // in-place update
                ArrayList<DataElement> stats = aggFields[i].asList();
                float value = newEltFields[i].asNumber().floatValue();
                stats.get(Stats.COUNT.ordinal()).addValue(1);
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
            flusher = new Flusher(temporalStore.getStorageEngine().getStorageEngineClient(temporalStore.getName(), windowName, groupByKeyAddress, aggFieldAddress),this);
        }
        // move all windows to flush queue starting from startWindow
        for (int i=0; i<windows.length; i++) {
            int current = (i+startWindow) % MAX_RETENTION_WINDOWS;
            flusher.send(windows[current]);
            windows[current]=null;
        }
    }


}
