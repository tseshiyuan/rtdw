package com.saggezza.lubeinsights.platform.core.datastore;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.kafka.KafkaProducer;
import com.saggezza.lubeinsights.platform.core.common.kafka.KafkaUtil;
import com.saggezza.lubeinsights.platform.core.serviceutil.*;
import org.apache.log4j.Logger;

import java.util.ArrayList;

/**
 * Created by chiyao on 10/3/14.
 */

/**
 * upsert data via kafka
 */
public class StorePublisher {

    public static final Logger logger = Logger.getLogger(StorePublisher.class);
    public static final ServiceConfig config = ServiceConfig.load();
    protected String dataStoreName;
    protected DataModel dataModel;
    protected KafkaProducer kafkaProducer = null;

    public StorePublisher(String dataStoreName, DataModel dataModel) {
        this.dataStoreName = dataStoreName;
        this.dataModel = dataModel;
    }

    /**
     * send an open request to data store manager to set up receiving end
     * also init kafka producer
     *
     * @return true if succeed
     */
    public void open() {
        String topic = getTopic();
        kafkaProducer = new KafkaProducer(topic);
    }

    /**
     * notify data store manager to close it
     * also shut down kafka consumer
     */
    public void close() {
        if (kafkaProducer != null) {
            kafkaProducer.close();
            kafkaProducer = null;
        }
   }

    /**
     * send an EOB token
     */
    public void endBatch() {
        kafkaProducer.send(KafkaUtil.EOB); // send end of batch so that receiver can close the consumer
    }

    private final String getTopic() {
        return StorageUtil.getTableName(config.get("tenant"),config.get("application"),dataStoreName);
    }

    /**
     * assuming called after open()
     * @param element
     */
    public final void upsert(String batchId, DataElement element) {
        if (logger.isDebugEnabled()) {
            logger.debug("StorePublisher upsert:" + element);
        }
        kafkaProducer.send(batchId+"_"+element.toString());  // short notion
    }

    public static final void main(String[] args) {
        StorePublisher client = null;
        try {

            DataStore store = DataStoreCatalog.getDataStore(config.get("tenant"), config.get("application"), "store01");
            client = new StorePublisher("store01", store.getDataModel());
            System.out.println(store.serialize());
            System.out.println(store.getDataModel().toJson());
            client.open();
            ArrayList<DataElement> al = StorageUtil.readFromFile(args[0],store.getDataModel());
            for (DataElement e: al) {
                client.upsert("testBatchId",e);
                //System.out.println(e);
            }

            //test
            /*
            KafkaProducer producer = new KafkaProducer("FileCollector.myCollector.2");
            Date date = new Date(System.currentTimeMillis());
            producer.send(date.toString());
            producer.send(KafkaUtil.EOB);
            producer.close();
            */

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
