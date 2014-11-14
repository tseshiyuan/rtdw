package com.saggezza.lubeinsights.platform.core.common.kafka;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.Date;

/**
 * Created by chiyao on 9/5/14.
 */
public class KafkaProducer implements AutoCloseable {

    public static final Logger logger = Logger.getLogger(KafkaProducer.class);
    protected String topic;
    protected ProducerConfig config;
    protected Producer<String, String> producer;

    public KafkaProducer(String topic) {
        this.topic = topic;
        config =  KafkaUtil.getProducerConfig();
        producer = new Producer<String, String>(config);
    }


    public void send(String msg) {
        //KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg, msg);  // topic,key,msg
        if (logger.isDebugEnabled()) {
            logger.debug("Producer " + msg);
        }
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, msg);  // topic,msg (key is a null obj))
        producer.send(data);
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

    public static final void main(String[] args) {
        try {
            //KafkaUtil.setTopic("FileCollector.myCollector"); // create topic if not exists yet
            KafkaProducer producer = new KafkaProducer("FileCollector.myCollector.1");
            Date date = new Date(System.currentTimeMillis());
            producer.send(date.toString());
            producer.send(KafkaUtil.EOB);
            producer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
