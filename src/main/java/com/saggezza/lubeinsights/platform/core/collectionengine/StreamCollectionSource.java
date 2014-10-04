package com.saggezza.lubeinsights.platform.core.collectionengine;

/**
 * Created by chiyao on 8/25/14.
 */

import com.saggezza.lubeinsights.platform.core.common.kafka.KafkaConsumer;
import com.saggezza.lubeinsights.platform.core.common.kafka.KafkaUtil;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A CollectionSource represents a source of data batch
 * It could be a file or a KAFKA topic with a data scope
 */
public class StreamCollectionSource implements AutoCloseable {

    public static final Logger logger = Logger.getLogger(StreamCollectionSource.class);

    // kafka property
    protected KafkaConsumer kafkaConsumer;
    protected ExecutorService executor;
    protected LinkedBlockingQueue<String> dataQueue=null;


    protected String desc;
    protected String groupId;
    protected boolean stopSignal = false;

    public final boolean stopSignaled() {
        return stopSignal;
    }


    /**
     * sent by CollectionEngine to stop collecting
     */
    public final void stopCollectingStream() {
        stopSignal = true;
    }


    public StreamCollectionSource(String desc, String groupId) {
        this.desc = desc;
        this.groupId = groupId;
    }

    /**
     * close or shutdown this CollectionSource
     */
    public final void close() {
        if (kafkaConsumer != null) {  // TODO: check status
            kafkaConsumer.close();
        }
        if (executor != null) {
            executor.shutdown();
        }
        logger.info("Closed StreamCollectionSource "+ desc + " of group "+groupId);
    }

    /**
     * start the collection source
     * @return a LinkedBlockedQueue that contains the result data stream
     */
    public void start() {
        if (kafkaConsumer == null) {
            kafkaConsumer = new KafkaConsumer(groupId,false); // just use one group for now
        }
        dataQueue = kafkaConsumer.start(desc);
    }

    /**
     * get next record/message, called by CollectionEngine
     * @return
     */
    public final String nextRec() {
        try {
            return dataQueue.take();
        } catch (Exception e) {
            logger.trace("StreamCollectionSource error", e);
            throw new RuntimeException(e);
        }
    }

    public void commit() {
        kafkaConsumer.commit();
    }

    public static void main(String[] args) {
        String groupId = String.valueOf(System.currentTimeMillis());
        try (StreamCollectionSource streamCollectionSource = new StreamCollectionSource("FileCollector.activity.activity-log.1",groupId)) {
            streamCollectionSource.start();
            for (String rec = streamCollectionSource.nextRec(); !rec.equals(KafkaUtil.EOB); rec = streamCollectionSource.nextRec()) {
                System.out.println(rec);
            }
            System.out.println("Done collecting stream");
        }
    }


}
