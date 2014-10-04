package com.saggezza.lubeinsights.platform.core.collectionengine;

/**
 * Created by chiyao on 8/25/14.
 */

import com.saggezza.lubeinsights.platform.core.common.kafka.KafkaConsumer;
import com.saggezza.lubeinsights.platform.core.common.kafka.KafkaUtil;

import java.util.concurrent.LinkedBlockingQueue;
import org.apache.log4j.Logger;

/**
 * A BatchCollectionSource represents a source of data batch
 * It is represented by a KAFKA topic with a data scope
 */
public class BatchCollectionSource implements AutoCloseable {

    public static final Logger logger = Logger.getLogger(BatchCollectionSource.class);

    protected KafkaConsumer kafkaConsumer;
    protected String desc;
    protected LinkedBlockingQueue<String> dataQueue;


    public BatchCollectionSource(String desc) {
        this.desc = desc;
    }

    /**
     * close or shutdown this CollectionSource
     */
    public final void close() {
        if (kafkaConsumer != null) {  // TODO: check status
            System.out.println("closing kafkaConsumer");
            kafkaConsumer.close();
        }
    }

    /**
     * called by CollectionEngine
     * done with the batch, so commitOffsets
     */
    public void doneBatch(String batchId, boolean cleanup) {
        kafkaConsumer.commit();
        if (cleanup) {
            String topic = KafkaUtil.getTopic(desc,batchId);
            KafkaUtil.cleanTopic(topic);
        }
    }

    /**
     * Get a batch of data in form of LinkedBlockingQueue
     * by getting from the topic <desc>.<batchId>
     * @param batchId
     * @return  LinkedBlockingQueue whose content ends with EOB, or null if topic does not exist
     */
    public final LinkedBlockingQueue<String> getBatch(String batchId) {
        // check if topic exists
        String topic = KafkaUtil.getTopic(desc, batchId);
        logger.info("Collection source get data from topic "+topic);
        if (!KafkaUtil.topicExists(topic)) {
            logger.warn("Batch "+ batchId+ " for source "+desc+" doesn't exist");
            return null;
        }
        // start the consumer if not started yet
        if (kafkaConsumer == null) {
            // use current time as the NEW groupId so that it always reads the topic from the beginning
            kafkaConsumer = new KafkaConsumer(String.valueOf(System.currentTimeMillis()),true);
        }
        LinkedBlockingQueue<String> dataQueue = kafkaConsumer.start(topic);
        return dataQueue; // caller will get result from this dataQueue
    }

    public static final void main(String[] args) {
        try (BatchCollectionSource source = new BatchCollectionSource("FileCollector.myCollector")) {
            LinkedBlockingQueue<String> batch = source.getBatch("activity-log");
            if (batch==null) {
                System.out.println("Batch unavailable");
                return;
            }
            for (String rec = batch.take(); !rec.equals(KafkaUtil.EOB); rec = batch.take()) {
                System.out.println(rec);
            }
            source.doneBatch("activity-log",false); // don't clean up
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
