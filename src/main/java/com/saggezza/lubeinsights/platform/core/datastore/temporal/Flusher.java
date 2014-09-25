package com.saggezza.lubeinsights.platform.core.datastore.temporal;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.datastore.StorageEngineClient;
import com.saggezza.lubeinsights.platform.core.datastore.temporal.ByteArray;
import com.saggezza.lubeinsights.platform.core.datastore.temporal.CacheStore;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Created by chiyao on 9/19/14.
 */
public class Flusher {

    public static final int FRESHNESS = 2; // wait for no more than 2 hours to flush
    private LinkedBlockingQueue<ConcurrentHashMap<ByteArray,DataElement>> queue;
    ExecutorService executor = Executors.newFixedThreadPool(1);

    public Flusher(StorageEngineClient storageEngineClient, CacheStore cacheStore) {
        queue = new LinkedBlockingQueue<ConcurrentHashMap<ByteArray, DataElement>>();
        Runnable worker = new Runnable() {
            @Override
            public void run() {
                try {
                    // initialize client
                    storageEngineClient.open();
                    // TODO: handle shutdown
                    ConcurrentHashMap<ByteArray, DataElement> map;
                    // If idle for 2 hours, ask cacheStore to flush. Otherwise, process map
                    for (map = queue.poll(FRESHNESS,TimeUnit.HOURS);
                         map == null; map = queue.poll(FRESHNESS,TimeUnit.HOURS)) {
                        cacheStore.flusherRequest();
                    }
                    // flush the entire map (elements in window)
                    for (Map.Entry<ByteArray, DataElement> e : map.entrySet()) {
                        storageEngineClient.aggsert(e.getValue()); // aggregate to the same key entry, or insert if not exists
                    }
                } catch (Exception e) {
                    e.printStackTrace(); // TODO
                } finally {
                    if (storageEngineClient.isOpen()) {
                        try {
                            storageEngineClient.close();
                        } catch (IOException e) {
                            e.printStackTrace(); // TODO
                        }
                    }
                }
            }
        };
        executor.submit(worker);
    }

    public void send(ConcurrentHashMap<ByteArray,DataElement> map) {
        queue.offer(map);
    }



}
