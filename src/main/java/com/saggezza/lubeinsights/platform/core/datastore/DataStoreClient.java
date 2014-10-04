package com.saggezza.lubeinsights.platform.core.datastore;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.kafka.KafkaProducer;
import com.saggezza.lubeinsights.platform.core.common.kafka.KafkaUtil;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceCommand;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceConfig;

/**
 * Created by chiyao on 10/3/14.
 */
public class DataStoreClient {

    public static final ServiceConfig config = ServiceConfig.load();
    protected String dataStoreName;
    protected DataModel dataModel;
    protected KafkaProducer kafkaProducer = null;

    public DataStoreClient(String dataStoreName, DataModel dataModel) {
        this.dataStoreName = dataStoreName;
        this.dataModel = dataModel;
    }

    /**
     * send an open request to data store manager to set up receiving end
     * also init kafka producer
     * @return true if succeed
     */
    public void open() {
        sendCommand(ServiceCommand.OPEN_STORE, dataStoreName);
        String topic = config.get("tenant")+"."+config.get("application")+".datastore."+dataStoreName;
        kafkaProducer = new KafkaProducer(topic);
    }

    /**
     * notify data store manager to close it
     * also shut down kafka consumer
     */
    public void close() {
        sendCommand(ServiceCommand.CLOSE_STORE, dataStoreName);
        if (kafkaProducer != null) {
            kafkaProducer.send(KafkaUtil.EOB); // send end of batch so that receiver can close the consumer
            kafkaProducer.close();
            kafkaProducer = null;
        }
    }

    /**
     * send command to data store manager
     * @param command
     * @param param
     */
    public void sendCommand(ServiceCommand command, String param) {

    }

    /**
     * assuming called after open()
     * @param element
     */
    public final void upsert(DataElement element) {
        //kafkaProducer.send(element.toString());  // short notion
        kafkaProducer.send(element.serialize());   // long notion
    }

}
