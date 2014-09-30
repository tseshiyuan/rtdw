package com.saggezza.lubeinsights.platform.core.datastore;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;

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
        System.out.println("creating flusher");
        queue = new LinkedBlockingQueue<ConcurrentHashMap<ByteArray, DataElement>>();
        Runnable worker = new Runnable() {
            @Override
            public void run() {
                try {
                    System.out.println("entering client");
                    // initialize client
                    storageEngineClient.open();
                    while (true) {
                        ConcurrentHashMap<ByteArray, DataElement> map;
                        // If idle for 2 hours, ask cacheStore to flush. Otherwise, process map
                        for (map = queue.poll(FRESHNESS, TimeUnit.HOURS);
                             map == null; map = queue.poll(FRESHNESS, TimeUnit.HOURS)) {
                            System.out.println("Idled too long, flush now");
                            cacheStore.flushRequest();
                        }
                        // check if it's a shutdown signal (empty map)
                        if (map.isEmpty()) {
                            break;
                        }
                        else {
                            // flush the entire map (elements in window)
                            for (Map.Entry<ByteArray, DataElement> e : map.entrySet()) {
                                storageEngineClient.aggsert(e.getValue()); // aggregate to the same key entry, or insert if not exists
                            }
                        }
                    }
                    System.out.println("leaving client");

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
        System.out.println("offer");
        queue.offer(map);
    }

    /**
     * wait till flushing is done
     */
    public final void close() {
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (Exception e) {e.printStackTrace();} // don't handle it now
    }
}
