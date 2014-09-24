package com.saggezza.lubeinsights.platform.core.collectionengine.kafka;

import kafka.admin.AdminUtils;
//import kafka.utils.ZkUtils;
import kafka.consumer.ConsumerConfig;
import kafka.producer.ProducerConfig;
import kafka.admin.DeleteTopicCommand;
import kafka.admin.TopicCommand;
import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;
import java.io.FileInputStream;
import java.util.Properties;

/**
 * Created by chiyao on 9/5/14.
 */
public class KafkaUtil {

    public static final Logger logger = Logger.getLogger(KafkaUtil.class);
    public static final String EOB = "EndOfBatch";

    protected static Properties props = loadProperties();
    public static final ZkClient zkClient = new ZkClient(props.getProperty("zookeeper.connect")); // connectionTimeout = Integer.MAX_VALUE

    public static final Properties loadProperties() {
        try {
            Properties prop = new Properties();
            String confFileName = System.getProperty("kafka.conf");   // kafka config file name comes from system property kafka.conf
            prop.load(new FileInputStream(confFileName));
            return prop;
        } catch (Exception e) {
            logger.trace("loadProperties error",e);
            throw new RuntimeException(e);
        }
    }

    public static final ConsumerConfig getConsumerConfig(String groupId, boolean forBatch) {
        // If groupId is null, use default in config file.
        if (groupId == null) {
            return new ConsumerConfig(props);
        }
        // Otherwise duplicate the property with the custom groupId
        Properties newProps = new Properties();
        for (String key: props.stringPropertyNames()) {
            System.out.println("property: "+ key);
            newProps.setProperty(key, props.getProperty(key));
        }
        newProps.setProperty("group.id",groupId);
        if (forBatch) {
            newProps.setProperty("auto.offset.reset","smallest"); // always read from top in batch mode
            newProps.setProperty("auto.commit.enable","false");  // commit only when batch is done
        }
        return new ConsumerConfig(newProps);
    }

    public static final ProducerConfig getProducerConfig() {
        return new ProducerConfig(props);
    }


    public static final String getTopic(String sourceName, String batchId) {
        return new StringBuilder(sourceName).append('.')
                .append(batchId)
                .toString();
    }


    public static final boolean topicExists(String topic) {
        // TODO: check connected, if not, reconnect
        return AdminUtils.topicExists(zkClient, topic);
    }

    /**
     * create this topic if it does not exist
     * @param topic
     */
    public static final void setTopic(String topic) {
        // create a topic if not exist
        if (!AdminUtils.topicExists(zkClient,topic)) {
            // This API is buggy. Producer can't send to a topic created by it
            //int numBrokers = props.getProperty("metadata.broker.list").split(",").length;
            //AdminUtils.createTopic(zkClient, topic, 1, (numBrokers>3 ? 3 : numBrokers), new Properties()); // partition=1, replication=min(3,numBrokers)
            //AdminUtils.createTopic(zkClient, topic, 1, 1, new Properties());

            // use command line instead
            int numBrokers = props.getProperty("metadata.broker.list").split(",").length;
            String repFactor = String.valueOf(numBrokers>3 ? 3 : numBrokers);
            String [] arguments = new String[] {
                "--create", "--zookeeper", props.getProperty("zookeeper.connect"), "--topic", topic,
                "--partitions","1","--replication-factor",repFactor
            };
            TopicCommand.main(arguments);
        }
    }

    /**
     * delete this topic
     * @param topic
     */
    public static final void cleanTopic(String topic) {
        // This api is buggy
        //AdminUtils.deleteTopic(zkClient,topic);

        // This does not have an API, so use command line
        //DeleteTopicCommand --zookeeper localhost:2181 --topic test
        String[] args = new String[] { "--zookeeper", props.getProperty("zookeeper.connect"), "--topic", topic};
        DeleteTopicCommand.main(args);
        //DeleteTopicCommand$.MODULE$.main(args);

        // This works, but it hacks
        //zkClient.deleteRecursive(ZkUtils.getTopicPath(topic));
    }

    public static final void main(String[] args) {
        //setTopic("test1");
        /*
        Properties config = AdminUtils.fetchTopicConfig(zkClient, "testb");
        for (String k: config.stringPropertyNames()) {
            System.out.println(k+ " -> "+config.getProperty(k));
        }
        */
        String topic = "testa";
        boolean exist = AdminUtils.topicExists(zkClient,topic);
        System.out.println("Topic "+topic+" exist? " + exist);

        if (!exist) {
            AdminUtils.createTopic(zkClient, topic, 1, 1, new Properties());
            System.out.println(topic + " created");
            exist = AdminUtils.topicExists(zkClient, topic);
            System.out.println("Topic " + topic + " exist? " + exist);
        }

        cleanTopic(topic);
        System.out.println(topic+" deleted");
        exist = AdminUtils.topicExists(zkClient,topic);
        System.out.println("Topic " +topic+" exist? " + exist);
    }

}
