package com.saggezza.lubeinsights.platform.apps.datapipe;

import com.saggezza.lubeinsights.platform.core.common.ConditionExpression;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRefType;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngine;
import com.saggezza.lubeinsights.platform.core.serviceutil.*;
import com.saggezza.lubeinsights.platform.core.workflowengine.Node;
import com.saggezza.lubeinsights.platform.core.workflowengine.NodeWork;
import com.saggezza.lubeinsights.platform.core.workflowengine.WorkFlow;
import com.saggezza.lubeinsights.platform.core.workflowengine.WorkFlowEngine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static com.google.common.collect.Sets.newHashSet;

/**
 * @author : Albin
 */
public class DataEngineTest {


    static final String uppcasedOutput = "uppcasedOutput";
    static final String infile = "infile";

    private static void dataEngineLocalTest() {
        ArrayList<ServiceRequest.ServiceStep> steps = serviceSteps(true);

        DataEngine dataEngine = new DataEngine();
        ServiceResponse serviceResponse = dataEngine.processRequest(new ServiceRequest(steps));
        DataChannel data = serviceResponse.getData();

        for(String each : data.getTags()){
            String file = data.getDataRef(each).getFileName();
            System.out.println(file);
            try {
                List<String> strings = Files.readAllLines(Paths.get(file));
                System.out.println(strings);
            } catch (IOException e) {
                throw new RuntimeException("failed..",e);
            }
        }
    }

    private static void dataEngineRemoteTest() throws Exception {
        DataEngine dataEngine = new DataEngine();
        dataEngine.start(8082);


        ServiceGateway gateway = ServiceGateway.getServiceGateway();
        ServiceResponse serviceResponse = gateway.sendRequest(ServiceName.DATA_ENGINE,
                new ServiceRequest(serviceSteps(true)));

        System.out.println(serviceResponse.getData().getTags().iterator().next());

        gateway.close();
        dataEngine.stop();

        System.exit(0);
    }

    private static ArrayList<ServiceRequest.ServiceStep> serviceSteps2(final String inputTag) {
        ArrayList<ServiceRequest.ServiceStep> steps = new ArrayList<>();

        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Input, Params.of(inputTag)));

        final String filteredFile = "filteredFile";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Filter,
                Params.of(inputTag, filteredFile, filterCondition2().toJson())));

        final String lowerCasedOutput = "lowerCasedOutput";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map,
                Params.of(filteredFile, lowerCasedOutput, "LowerCase")));

        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Output,
                Params.of(lowerCasedOutput)));
        return steps;
    }

    private static DataChannel inChannel(){
        DataChannel in = new DataChannel();
        in.putDataRef(infile, new DataRef(DataRefType.FILE, "spark://local/Users/albin/Desktop/sample.csv"));
        return in;
    }

    private static ArrayList<ServiceRequest.ServiceStep> serviceSteps(boolean standAlone) {
        ArrayList<ServiceRequest.ServiceStep> steps = new ArrayList<>();

        if(standAlone){
            DataChannel in = inChannel();
            steps.add(new ServiceRequest.ServiceStep(ServiceCommand.DefineInput, Params.of(in.toJson())));
        }

        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Input, Params.of(infile)));

        final String filteredFile = "filteredFile";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Filter,
                Params.of(infile, filteredFile, filterCondition1().toJson())));

        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map,
                Params.of(filteredFile, uppcasedOutput, "UpperCase")));

        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Output,
                Params.of(uppcasedOutput)));
        return steps;
    }

    private static void workFlowBasedTest() throws Exception {
        WorkFlowEngine workFlowEngine = new WorkFlowEngine();
        workFlowEngine.start(8081);
//
        DataEngine engine = new DataEngine();
        engine.start(8082);

        NodeWork work1 = new NodeWork(ServiceName.DATA_ENGINE, new ServiceRequest(serviceSteps(false)));
        NodeWork work2 = new NodeWork(ServiceName.DATA_ENGINE, new ServiceRequest(serviceSteps2(uppcasedOutput)));

        Node node1 = new Node("UpperCase Filtering", work1);
        Node node2 = new Node("LowerCase Filtering", work2);
        HashSet<Node> nodes = newHashSet(node1, node2);

        WorkFlow workFlow = new WorkFlow("workflow", nodes);
        workFlow.addLink(node1, node2);

        ServiceGateway gateway = new ServiceGateway();
        ServiceResponse serviceResponse = gateway.sendRequest(ServiceName.WORKFLOW_ENGINE,
                workFlow.toServiceRequest(inChannel()));

        for(String each : serviceResponse.getData().getTags()){
            System.out.println(each);
            System.out.println(serviceResponse.getData().getDataRef(each).getFileName());
            System.out.println("---------------------------------------------------");
        }
        System.exit(0);
    }

    private static ConditionExpression filterCondition1(){
        ConditionExpression containsName = new ConditionExpression("Contains", Params.of("name"));
        ConditionExpression notContainsName = new ConditionExpression("Not", Params.of(containsName));
        return notContainsName;
    }

    private static ConditionExpression filterCondition2(){
        ConditionExpression containsName = new ConditionExpression("Contains", Params.of("CK"));
        ConditionExpression notContainsName = new ConditionExpression("Not", Params.of(containsName));
        return notContainsName;
    }

    public static void main(String[] args) throws Exception {
        workFlowBasedTest();
    }

}
