package com.saggezza.lubeinsights.platform.core.common.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by chiyao on 9/5/14.
 */

/**
 * This is a wrapper class to get kafka data. It will have a background thread constantly getting data from the specified topic until close() call
 */
public class KafkaConsumer implements AutoCloseable {

    public static final Logger logger = Logger.getLogger(KafkaConsumer.class);
    protected ConsumerConnector consumerConnector;
    protected ExecutorService executor;

    public KafkaConsumer(String groupId, boolean forBatch) {
        consumerConnector = Consumer.createJavaConsumerConnector(KafkaUtil.getConsumerConfig(groupId,forBatch)); // just use one group for now
    }

    /*
    public KafkaConsumer() {
        this(null);
    }
    */

    public void commit() {
        consumerConnector.commitOffsets();
    }

    public void close() {
        try {
            consumerConnector.shutdown();
            if (executor != null) {
                executor.shutdown();
                if (executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    logger.info("KafkaConsumer's executor's tasks terminated");
                }
                else {
                    executor.shutdownNow();
                    logger.info("Timed out waiting. Forced KafkaConsumer's executor to shutdown");
                }
            }
            logger.info("KafkaConsumer closed");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public LinkedBlockingQueue<String> start(String topic) {
        System.out.println("KafkaConsumer to collect data from topic "+topic);
        LinkedBlockingQueue<String> result = new LinkedBlockingQueue<String>();
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        // set up only 1 stream
        if (streams == null) {
            return result;
        }
        //System.out.println("Streams count = "+streams.size());
        KafkaStream stream = streams.get(0);
        executor = Executors.newFixedThreadPool(1);
        Runnable job = new Runnable() {
            public void run() {
                //System.out.println("run listener");
                ConsumerIterator<byte[], byte[]> it = stream.iterator();
                while (it.hasNext()) {   // this call will block on empty stream until consumerConnector shutdown
                    // TODO: time out waiting
                    String msg = new String(it.next().message());
                    if (logger.isDebugEnabled()) {
                        logger.debug("From consumer stream: " + msg);
                    }
                    if (msg.equals(KafkaUtil.EOB)) {
                        commit(); // commit iff EOB, regardless whether in auto-commit mode or not
                    }
                    result.offer(msg); // non-blocking insert
                }
                logger.info("Consumer listener loop ends");
            }
        };
        executor.submit(job);
        return result;
    }

    public static final void main(String[] args) {
        try {
            String groupId = String.valueOf(System.currentTimeMillis());
            groupId = "test1";
            KafkaConsumer consumer = new KafkaConsumer(groupId, true);
            //consumer.start("FileCollector.myCollector.activity-log", queue);
            LinkedBlockingQueue<String> queue = consumer.start("tenantTest_appTest_store01");
            String msg;
            msg = queue.take();
            System.out.println(msg);
            /*
            while ((msg = queue.take()) != null) { // blocking call
                System.out.println(msg);
                if (msg.equalsIgnoreCase(KafkaUtil.EOB)) {
                    break;
                }
            }
            */
            consumer.commit();
            System.out.println("done");
            consumer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
