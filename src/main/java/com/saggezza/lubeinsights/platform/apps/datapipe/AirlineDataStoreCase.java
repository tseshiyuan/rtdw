package com.saggezza.lubeinsights.platform.apps.datapipe;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.Selection;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngine;
import com.saggezza.lubeinsights.platform.core.datastore.DataStoreManager;
import com.saggezza.lubeinsights.platform.core.serviceutil.*;
import com.saggezza.lubeinsights.platform.core.workflowengine.Node;
import com.saggezza.lubeinsights.platform.core.workflowengine.NodeWork;
import com.saggezza.lubeinsights.platform.core.workflowengine.WorkFlow;
import com.saggezza.lubeinsights.platform.core.workflowengine.WorkFlowEngine;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static com.google.common.collect.Sets.newHashSet;
import static com.saggezza.lubeinsights.platform.apps.datapipe.AirlineJoinAndGroupBy.*;

/**
 * @author : Albin
 */
public class AirlineDataStoreCase {

    static Properties appProperties = AirlineJoinAndGroupBy.appProperties;
    static final String MaxCarrierStore = "maxCarrierStore";
    static final String ParsedStore = "parsedStore9";
    static final String DerivedStore = "DerivedStore";


    static final WorkFlowEngine workFlowEngine = new WorkFlowEngine();
    static final DataEngine engine = new DataEngine();
    static final DataStoreManager dataStoreManager = new DataStoreManager();


    static ArrayList<ServiceRequest.ServiceStep> serviceStepsMod2(){
        ArrayList<ServiceRequest.ServiceStep> serviceSteps = serviceSteps2(false);
        serviceSteps.remove(serviceSteps.size() - 1);
        serviceSteps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(maxCarrierInfo, maxCarrierInfo, "Select",
                new Selection(new int[]{1, 2, 3, 4}, new String[]{"blah1", "blah2", "blah3", "blah4"}))));
        serviceSteps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(parsedResult, parsedResult, "Select",
                new Selection(new int[]{0,1,2,3,4,5,6,7,8,9,10,11,12}, new String[]{"col1","col2","col3","col4","col5","col6","col7","col8","col9","col10", "col11", "col12", "col13"}))));
        serviceSteps.add(new ServiceRequest.ServiceStep(ServiceCommand.Write,
                Params.ofPairs(AirlineJoinAndGroupBy.maxCarrierInfo, MaxCarrierStore, AirlineJoinAndGroupBy.parsedResult, ParsedStore)));
        return serviceSteps;
    }

    static void testCase(boolean standAlone) throws Exception{
        if(standAlone){
            startEngines();
        }

        setUpDataStores();
        setUpDerivedStoreAndStart();

        NodeWork work1 = new NodeWork(ServiceName.DATA_ENGINE, new ServiceRequest(serviceSteps(false)));
        NodeWork work2 = new NodeWork(ServiceName.DATA_ENGINE, new ServiceRequest(serviceStepsMod2()));

        Node node1 = new Node("Carrier with Max number of flights", work1);
        Node node2 = new Node("All Info of max flights carrier", work2);
        HashSet<Node> nodes = newHashSet(node1, node2);

        WorkFlow workFlow = new WorkFlow("All Info of max flight carrier", nodes);
        workFlow.addLink(node1, node2);

        System.out.println(workFlow.toJson());
        ServiceGateway gateway = ServiceGateway.getServiceGateway();
        ServiceResponse serviceResponse = gateway.sendRequest(ServiceName.WORKFLOW_ENGINE,
                workFlow.toServiceRequest(inChannel()));

        DataRef dataRef = null;
        for(String each : serviceResponse.getData().getTags()){
            System.out.println(each);
            System.out.println(serviceResponse.getData().getDataRef(each).getFileName());
            if(each.startsWith(allInfoOfMaxCarrier)){
                dataRef = serviceResponse.getData().getDataRef(each);
            }
            System.out.println("---------------------------------------------------");
        }

        if(standAlone){
            stopEngines();
        }
    }

    private static void stopEngines() throws Exception {
        workFlowEngine.stop();
        engine.stop();
        dataStoreManager.stop();
    }

    private static void startEngines() throws Exception {
        workFlowEngine.start(9981);
        engine.start(9982);
        dataStoreManager.start(9983);
    }


    static void testCaseWithStoreWriteAndRead() throws Exception{
        WorkFlowEngine workFlowEngine = new WorkFlowEngine();
        workFlowEngine.start(9981);
        DataEngine engine = new DataEngine();
        engine.start(9982);
        DataStoreManager dataStoreManager = new DataStoreManager();
        dataStoreManager.start(9983);

        setUpDataStores();

        NodeWork work1 = new NodeWork(ServiceName.DATA_ENGINE, new ServiceRequest(serviceSteps(false)));
        NodeWork work2 = new NodeWork(ServiceName.DATA_ENGINE, new ServiceRequest(serviceStepsMod2()));

        Node node1 = new Node("Carrier with Max number of flights", work1);
        Node node2 = new Node("All Info of max flights carrier", work2);
        HashSet<Node> nodes = newHashSet(node1, node2);

        WorkFlow workFlow = new WorkFlow("All Info of max flight carrier", nodes);
        workFlow.addLink(node1, node2);

        ServiceGateway gateway = ServiceGateway.getServiceGateway();
        ServiceResponse serviceResponse = gateway.sendRequest(ServiceName.WORKFLOW_ENGINE,
                workFlow.toServiceRequest(inChannel()));

        DataRef dataRef = null;
        for(String each : serviceResponse.getData().getTags()){
            System.out.println(each);
            System.out.println(serviceResponse.getData().getDataRef(each).getFileName());
            if(each.startsWith(allInfoOfMaxCarrier)){
                dataRef = serviceResponse.getData().getDataRef(each);
            }
            System.out.println("---------------------------------------------------");
        }

        workFlowEngine.stop();
        engine.stop();
        dataStoreManager.stop();
    }

    static void testDerivedStoreCase() throws Exception {
        DataStoreManager dataStoreManager = new DataStoreManager();
        dataStoreManager.start(9983);

        setUpDerivedStoreAndStart();
    }

    static void deleteDS() throws Exception {
        ServiceGateway gateway = ServiceGateway.getServiceGateway();
        gateway.sendRequest(ServiceName.DATASTORE_MANAGER, new ServiceRequest(ServiceCommand.DELETE_STORE, Params.ofPairs("name", MaxCarrierStore)));
        gateway.sendRequest(ServiceName.DATASTORE_MANAGER, new ServiceRequest(ServiceCommand.DELETE_STORE, Params.ofPairs("name", ParsedStore)));
    }

    static void deleteDDS() throws Exception {
        ServiceGateway gateway = ServiceGateway.getServiceGateway();
        gateway.sendRequest(ServiceName.DATASTORE_MANAGER, new ServiceRequest(ServiceCommand.STOP_ALL_DERIVED_STORES, Params.None));
        gateway.sendRequest(ServiceName.DATASTORE_MANAGER, new ServiceRequest(ServiceCommand.DELETE_STORE, Params.ofPairs("name", DerivedStore)));
    }

    static void setUpDataStores() throws Exception {
        ServiceGateway gateway = ServiceGateway.getServiceGateway();
        ServiceResponse serviceResponse = gateway.sendRequest(ServiceName.DATASTORE_MANAGER,
                new ServiceRequest(ServiceCommand.NEW_STORE,
                Params.ofPairs("name", MaxCarrierStore, "dataModel", allInfoStore(), "indexFields",
                        new String[]{"blah1"}, "force", true)));
        if(!serviceResponse.getStatus().equals("OK")){
            throw new RuntimeException("Cannot add data store");
        }

        serviceResponse = gateway.sendRequest(ServiceName.DATASTORE_MANAGER, new ServiceRequest(ServiceCommand.NEW_STORE,
                Params.ofPairs("name", ParsedStore, "dataModel", parsedModelStore(), "indexFields",
                        new String[]{"col5", "col6", "col7", "col8", "col10"}, "force", true)));
        if(!serviceResponse.getStatus().equals("OK")){
            throw new RuntimeException("Cannot add data store");
        }
    }

    private static void setUpDerivedStoreAndStart() throws Exception {
        ServiceGateway gateway = ServiceGateway.getServiceGateway();
        ServiceResponse serviceResponse;
        serviceResponse = gateway.sendRequest(ServiceName.DATASTORE_MANAGER, new ServiceRequest(ServiceCommand.NEW_DERIVED_STORE,
                Params.ofPairs("name", DerivedStore, "forName", ParsedStore, "temporalKey", null,
                        "groupByKeys",
                        new String[]{"col1", "col9"}, "aggFields", new String[]{"col10"}, "filterName", null, "force", true)));
        if(!serviceResponse.getStatus().equals("OK")){
            throw new RuntimeException("Cannot add data store");
        }

        serviceResponse = gateway.sendRequest(ServiceName.DATASTORE_MANAGER, new ServiceRequest(ServiceCommand.START_DERIVED_STORE,
                Params.ofPairs("name", DerivedStore)));
        if(!serviceResponse.getStatus().equals("OK")){
            throw new RuntimeException("Cannot start data store");
        }
    }

    static DataModel parsedModelStore(){
        LinkedHashMap<String, DataModel> map = new LinkedHashMap<>();
        for(int i=1; i < 14; i++){
            map.put("col"+i, new DataModel(DataType.TEXT));
        }
        return new DataModel(map);
    }

    static DataModel allInfoStore(){
        LinkedHashMap<String, DataModel> map = new LinkedHashMap<>();
        map.put("blah1", new DataModel(DataType.TEXT));
        map.put("blah2", new DataModel(DataType.NUMBER));
        map.put("blah3", new DataModel(DataType.TEXT));
        map.put("blah4", new DataModel(DataType.NUMBER));
        return new DataModel(map);
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("service.conf", appProperties.getProperty("serviceFile"));
        System.setProperty("kafka.conf", appProperties.getProperty("kafkaFile"));

        testCase(false);
//        testDerivedStoreCase();

    }

}
