package com.saggezza.lubeinsights.platform.core.datastore;

import com.saggezza.lubeinsights.platform.core.common.Params;
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
public class DataStoreClientIndirect {

    public static final Logger logger = Logger.getLogger(DataStoreClientIndirect.class);
    public static final ServiceConfig config = ServiceConfig.load();
    protected String dataStoreName;
    protected DataModel dataModel;
    protected KafkaProducer kafkaProducer = null;

    public DataStoreClientIndirect(String dataStoreName, DataModel dataModel) {
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
//        sendCommand(ServiceCommand.OPEN_STORE_R, Params.of(dataStoreName, topic));
        kafkaProducer = new KafkaProducer(topic);

        // TODO: support write mode
    }

    /**
     * notify data store manager to close it
     * also shut down kafka consumer
     */
    public void close() {
        if (kafkaProducer != null) {
            kafkaProducer.send(KafkaUtil.EOB); // send end of batch so that receiver can close the consumer
            kafkaProducer.close();
            kafkaProducer = null;
        }
        String topic = getTopic();
//        sendCommand(ServiceCommand.CLOSE_STORE, Params.of(dataStoreName, topic));
    }

    private final String getTopic() {
        // TODO: add client id and batch id to topic
        return config.get("tenant")+"."+config.get("application")+".datastore."+dataStoreName;
    }


    /**
     * send command to data store manager
     * @param command
     * @param param
     */
    public void sendCommand(ServiceCommand command, Params param) {
        try {
            ServiceGateway serviceGateway = ServiceGateway.getServiceGateway();
            ServiceRequest request = new ServiceRequest(command, param);
            serviceGateway.sendRequest(ServiceName.DATASTORE_MANAGER, request);
        } catch (Exception e) {
            e.printStackTrace();
            logger.trace(e.getMessage(),e);
            throw new RuntimeException(e);
        }
    }

    /**
     * assuming called after open()
     * @param element
     */
    public final void upsert(DataElement element) {
        kafkaProducer.send(element.toString());  // short notion
        //kafkaProducer.send(element.serialize());   // long notion
    }

    public static final void main(String[] args) {
        DataStoreClientIndirect client = null;
        try {
            DataStore store = DataStoreCatalog.getDataStore(config.get("tenant"), config.get("application"), "store1");
            client = new DataStoreClientIndirect("store1", store.getDataModel());
            System.out.println(store.serialize());
            System.out.println(store.getDataModel().toJson());
            client.open();
            ArrayList<DataElement> al = DataStoreManager.readFromFile(args[0],store.getDataModel());
            for (DataElement e: al) {
                client.upsert(e);
                System.out.println(e);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
