package com.saggezza.lubeinsights.platform.apps.datapipe;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.Selection;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngine;
import com.saggezza.lubeinsights.platform.core.datastore.DataStoreManager;
import com.saggezza.lubeinsights.platform.core.serviceutil.*;
import com.saggezza.lubeinsights.platform.core.workflowengine.Node;
import com.saggezza.lubeinsights.platform.core.workflowengine.NodeWork;
import com.saggezza.lubeinsights.platform.core.workflowengine.WorkFlow;
import com.saggezza.lubeinsights.platform.core.workflowengine.WorkFlowEngine;

import java.util.ArrayList;
import java.util.HashSet;

import static com.google.common.collect.Sets.newHashSet;
import static com.saggezza.lubeinsights.platform.apps.datapipe.AirlineDataStoreCase.*;
import static com.saggezza.lubeinsights.platform.apps.datapipe.AirlineJoinAndGroupBy.*;
import static com.saggezza.lubeinsights.platform.apps.datapipe.AirlineJoinAndGroupBy.appProperties;

/**
 * @author : Albin
 */
public class AirlineDataStoreReadWriteCase {

    private static ArrayList<ServiceRequest.ServiceStep> modServiceSteps1(){
        ArrayList<ServiceRequest.ServiceStep> serviceSteps = serviceSteps(false);
        serviceSteps.remove(serviceSteps.size()-1);
        serviceSteps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(maxCarrierInfo, maxCarrierInfo, "Select",
                new Selection(new int[]{1, 2, 3, 4}, new String[]{"blah1", "blah2", "blah3", "blah4"}))));
        serviceSteps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(parsedResult, parsedResult, "Select",
                new Selection(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
                        new String[]{"col1", "col2", "col3", "col4", "col5", "col6", "col7",
                                "col8", "col9", "col10", "col11", "col12", "col13"}))));
        serviceSteps.add(new ServiceRequest.ServiceStep(ServiceCommand.Write,
                Params.ofPairs(AirlineJoinAndGroupBy.maxCarrierInfo, MaxCarrierStore)));
        serviceSteps.add(new ServiceRequest.ServiceStep(ServiceCommand.Publish, Params.of(parsedResult)));
        return serviceSteps;
    }

    private static ArrayList<ServiceRequest.ServiceStep> modServiceSteps2(){
        ArrayList<ServiceRequest.ServiceStep> steps = new ArrayList<>();

        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Load, Params.of(maxCarrierInfo, parsedResult)));
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Join,
                Params.of(allInfoOfMaxCarrier, maxCarrierInfo, new Selection("blah3", "blah1"), parsedResult, new Selection("col1", "col9"))));
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Publish, Params.of(allInfoOfMaxCarrier)));
        return steps;
    }


    static void testCaseWithStoreWriteAndRead() throws Exception{
//        WorkFlowEngine workFlowEngine = new WorkFlowEngine();
//        workFlowEngine.start(9981);
//        DataEngine engine = new DataEngine();
//        engine.start(9982);
//        DataStoreManager dataStoreManager = new DataStoreManager();
//        dataStoreManager.start(9983);

        setUpDataStores();

        NodeWork work1 = new NodeWork(ServiceName.DATA_ENGINE, new ServiceRequest(modServiceSteps1()));
        NodeWork work2 = new NodeWork(ServiceName.DATA_ENGINE, new ServiceRequest(modServiceSteps2()));

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

        deleteDS();

//        workFlowEngine.stop();
//        engine.stop();
//        dataStoreManager.stop();
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("service.conf", appProperties.getProperty("serviceFile"));
        System.setProperty("kafka.conf", appProperties.getProperty("kafkaFile"));

        testCaseWithStoreWriteAndRead();
    }


}
