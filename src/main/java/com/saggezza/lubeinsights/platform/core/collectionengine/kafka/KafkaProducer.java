package com.saggezza.lubeinsights.platform.core.collectionengine.kafka;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Date;

/**
 * Created by chiyao on 9/5/14.
 */
public class KafkaProducer implements AutoCloseable {

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
            KafkaUtil.setTopic("FileCollector.myCollector"); // create topic if not exists yet
            KafkaProducer producer = new KafkaProducer("FileCollector.myCollector");
            Date date = new Date(System.currentTimeMillis());
            producer.send(date.toString());
            producer.send(KafkaUtil.EOB);
            producer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
