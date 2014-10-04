package com.saggezza.lubeinsights.platform.core.collectionengine;

import com.saggezza.lubeinsights.platform.core.common.kafka.KafkaProducer;
import com.saggezza.lubeinsights.platform.core.common.kafka.KafkaUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by chiyao on 9/5/14.
 */
public class FileCollector {

    protected String desc; // descriptor of this collector

    public FileCollector(String name) {
        desc = "FileCollector."+name;
    }

    /**
     * load a file onto Kafka data bus using desc as the topic and fileName as the batchId
     * This will overwrite previously loaded file with the same name
     * @param file
     */
    public void load(File file) throws IOException {
        loadBatch(file, file.getName());
    } // file name is the batch id

    /**
     * load a file onto Kafka data bus using desc as the topic and fileName.version as the batchId
     * This will load the file with the batchId fileName.version, where version is the epoch time
     * @param file
     */
    public void loadWithVersion(File file) throws IOException {
        long now = System.currentTimeMillis();
        loadBatch(file, file.getName()+"."+now);
    }

    private void loadBatch(File file, String batchId) throws IOException {
        String topic = desc+"."+batchId;
        KafkaUtil.setTopic(topic); // create topic if not exists yet
        // create kafkaProducer on demand
        KafkaProducer kafkaProducer = new KafkaProducer(topic);  // desc as kafka topic
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                kafkaProducer.send(line);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (kafkaProducer != null) {
                kafkaProducer.send(KafkaUtil.EOB); // add an EndOfBatch token at the end
                kafkaProducer.close();
            }
        }

    }

    public static void main(String[] args) {
        try {
            String myName = System.getProperty("collector.name");
            FileCollector fileCollector = new FileCollector(myName); // collect activity data
            fileCollector.load(new File(args[0]));
            System.out.println("FileCollector "+myName+" loaded " + args[0]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
