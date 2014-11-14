package com.saggezza.lubeinsights.platform.apps.datapipe;

import com.google.common.collect.Sets;
import com.saggezza.lubeinsights.platform.core.common.ConditionExpression;
import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.*;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngine;
import com.saggezza.lubeinsights.platform.core.serviceutil.*;
import com.saggezza.lubeinsights.platform.core.workflowengine.Node;
import com.saggezza.lubeinsights.platform.core.workflowengine.NodeWork;
import com.saggezza.lubeinsights.platform.core.workflowengine.WorkFlow;
import com.saggezza.lubeinsights.platform.core.workflowengine.WorkFlowEngine;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Sets.newHashSet;
import static com.saggezza.lubeinsights.platform.apps.datapipe.AirlineDataSet.*;

/**
 * @author : Albin
 */
public class AirlineJoinAndGroupBy {


    static final String infile = "infile";
    static final String parsedResult = "parsedResult";
    static final String maxCarrierInfo = "maxCarrierInfo";
    static final String allInfoOfMaxCarrier = "allInfoOfMaxCarrier";

    static Properties appProperties = new Properties();


    static {
        try {
            appProperties.load(AirlineJoinAndGroupBy.class.getResourceAsStream("/app.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

     static void testDataEngineAlone() throws Exception {
        DataEngine engine = new DataEngine();
        engine.start(8082);

        ServiceRequest request = new ServiceRequest(serviceSteps(true));

        ServiceResponse serviceResponse = engine.processRequest(request, null);

//        ServiceGateway gateway = new ServiceGateway();
//        ServiceResponse serviceResponse = gateway.sendRequest(ServiceName.DATA_ENGINE,
//                request);

        Set<String> tags = serviceResponse.getData().getTags();
        for(String tag : tags){
            System.out.println(tag);
            System.out.println(serviceResponse.getData().getDataRef(tag).getDataModel());
            System.out.println();
        }

        engine.stop();

    }

     static void testDataEngineModelTransform() throws Exception {
        DataEngine engine = new DataEngine();
        engine.start(8082);

         DataChannel dc = new DataChannel();
         dc.putDataRef(infile, new DataRef(new DataModel(DataType.TEXT)));

        ServiceRequest request = new ServiceRequest(serviceStepsWithJoin(dc));

        ServiceGateway gateway = ServiceGateway.getServiceGateway();
        ServiceResponse serviceResponse = gateway.sendRequest(ServiceName.DATA_ENGINE,
                request, DataEngine.DataModel);

         if("ERROR".equals(serviceResponse.getStatus())){
             System.err.println(serviceResponse.getMessage());
         }else{
             Set<String> tags = serviceResponse.getData().getTags();
             for(String tag : tags){
                 System.out.println(tag);
                 System.out.println(serviceResponse.getData().getDataRef(tag).getDataModel().getList());
                 System.out.println();
             }
         }

         engine.stop();

    }

    static void testCaseWithJoinOnMap() throws Exception {
        WorkFlowEngine workFlowEngine = new WorkFlowEngine();
        workFlowEngine.start(8081);
        DataEngine engine = new DataEngine();
        engine.start(8082);

        NodeWork work1 = new NodeWork(ServiceName.DATA_ENGINE, new ServiceRequest(serviceStepsWithJoin(null)));

        Node node1 = new Node("Test Join Node", work1);
        HashSet<Node> nodes = newHashSet(node1);

        WorkFlow workFlow = new WorkFlow("All Info of max flight carrier", nodes);

        ServiceGateway gateway = ServiceGateway.getServiceGateway();
        ServiceResponse serviceResponse = gateway.sendRequest(ServiceName.WORKFLOW_ENGINE,
                workFlow.toServiceRequest(inChannel()));

        DataRef dataRef = null;
        for(String each : serviceResponse.getData().getTags()){
            System.out.println(each);
            System.out.println(serviceResponse.getData().getDataRef(each).getFileName());
            System.out.println("---------------------------------------------------");
        }



    }

    static ArrayList<ServiceRequest.ServiceStep> serviceStepsWithJoin(DataChannel dc) {
        ArrayList<ServiceRequest.ServiceStep> steps = new ArrayList<>();

        if(dc != null){
            steps.add(new ServiceRequest.ServiceStep(ServiceCommand.DefineInput, Params.of(dc.toJson())));
        }

        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Load, Params.of(infile)));
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(infile, parsedResult, "CsvParse", ",",15)));//car1, fli;car2, fli

        final String is2008 = "is2008";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Filter, Params.of(parsedResult, is2008,
                new ConditionExpression("ColumnPredicate",
                        Params.of(Month, new ConditionExpression("Equals",
                                Params.of("6")))).toJson())));
        final String is2009 = "is2009";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Filter, Params.of(parsedResult, is2009,
                new ConditionExpression("ColumnPredicate",
                        Params.of(Month, new ConditionExpression("Equals",
                                Params.of("7")))).toJson())));

        final String selected2009 = "selected2009";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(is2009, selected2009, "Select",
                new Selection(new int[]{Month, UniqueCarrier, FlightNum}, new String[]{"Year","UniqueCarrier","FlightNum"}))));

        final String distinctSelected2009 = "distinctSelected2009";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Dedup, Params.of(selected2009, distinctSelected2009)));

        final String selected2008 = "selected2008";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(is2008, selected2008, "Select",
                new Selection(new int[]{Month, UniqueCarrier, FlightNum}, new String[]{"Year","UniqueCarrier","FlightNum"}))));

        final String distinctSelected2008 = "distinctSelected2008";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Dedup, Params.of(selected2008, distinctSelected2008)));

        final String joined = "joined";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Join,
                Params.of(joined, distinctSelected2008, new Selection("FlightNum"), distinctSelected2009, new Selection("FlightNum"))));

        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Publish, Params.of(joined)));
        return steps;
    }


    static void testCase() throws Exception {
//        WorkFlowEngine workFlowEngine = new WorkFlowEngine();
//        workFlowEngine.start(8081);
//        DataEngine engine = new DataEngine();
//        engine.start(8082);

        NodeWork work1 = new NodeWork(ServiceName.DATA_ENGINE, new ServiceRequest(serviceSteps(false)));
        NodeWork work2 = new NodeWork(ServiceName.DATA_ENGINE, new ServiceRequest(serviceSteps2(false)));

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

        ServiceResponse collectionResponse = gateway.sendRequest(
                ServiceName.DATA_ENGINE, new ServiceRequest(ServiceCommand.Browse, Params.of(dataRef)));
        DataRef singleDataRef = collectionResponse.getData().getSingleDataRef();
        List<DataElement> data = singleDataRef.getValue();
        for(DataElement element : data){
            System.out.println(element.toString());
        }

//        workFlowEngine.stop();
//        engine.stop();
    }

     static ArrayList<ServiceRequest.ServiceStep> serviceSteps2(boolean standAlone) {
        ArrayList<ServiceRequest.ServiceStep> steps = new ArrayList<>();
        HashSet<String> tags = Sets.newHashSet(maxCarrierInfo, parsedResult);
        if(standAlone){
            DataChannel channel = makeDataChannel(tags);
            steps.add(new ServiceRequest.ServiceStep(ServiceCommand.DefineInput, Params.of(channel.toJson())));
        }

        /*
            Load maxCarrierInfo, parsedResult;
            allInfoMaxCarrier = Join maxCarrierInfo key[0,1] parsedResult [0,8];
            Output allInfoMaxCarrier;
         */

        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Load, Params.of(maxCarrierInfo, parsedResult)));
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Join,
                Params.of(allInfoOfMaxCarrier, maxCarrierInfo, new Selection(0, 1), parsedResult, new Selection(Year, UniqueCarrier))));
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Publish, Params.of(allInfoOfMaxCarrier)));
        return steps;
    }

     static DataChannel makeDataChannel(HashSet<String> tags) {
        DataChannel channel = new DataChannel();
        File path = new File("/Users/Shared/temp/");
        File[] files = path.listFiles();
        for(String tag : tags){
            for(File each : files){
                if(each.getName().startsWith(tag)){
                    channel.putDataRef(tag, new DataRef(DataRefType.FILE, "spark://local"+each.getAbsolutePath()));
                }
            }
        }
        return channel;
    }

     static DataChannel inChannel(){
        DataChannel in = new DataChannel();
        in.putDataRef(infile, new DataRef(DataRefType.FILE,
                "spark:/"+appProperties.getProperty("infile"), new DataModel(DataType.TEXT)));
        return in;
    }


    static ArrayList<ServiceRequest.ServiceStep> serviceSteps(boolean standAlone) {
        ArrayList<ServiceRequest.ServiceStep> steps = new ArrayList<>();

        if(standAlone){
            DataChannel in = inChannel();
            steps.add(new ServiceRequest.ServiceStep(ServiceCommand.DefineInput, Params.of(in.toJson())));
        }

        /*
            input infile;
            parsedResult = map infile CsvParse "," 15;
            selectedResult = select [0, 8, 9] from parsedResult;
            dedupedSelected = dedup selectedResult;
            countedFlights = map dedupSelected ColumnTransform 2 Constant 1;
            summedByYearAndCarrier = groupBy countedFlights key[0, 1] aggregation[{2, Sum}];
            maxOfFlights = groupBy summedByYearAndCarrier key[0] aggregation[{2, Max}];
            maxCarrierInfo = Join maxOfFlights key[2], summedByYearAndCarrier key[1];
            Output maxCarrierInfo, parsedResult
         */

        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Load, Params.of(infile)));
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(infile, parsedResult, "CsvParse", ",",15)));//car1, fli;car2, fli
        final String filteredFile = "filteredFile";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Filter, Params.of(parsedResult, filteredFile,
                expression().toJson())));
        final String selectedResult = "selectedResult";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(filteredFile, selectedResult, "Select",
                new Selection(Year, UniqueCarrier, FlightNum))));

        final String deupedSelected = "deupedSelected";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Dedup, Params.of(selectedResult, deupedSelected)));
        final String countedFlights = "countedFlights";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map,
                Params.of(deupedSelected, countedFlights, "ColumnTransform", 2, "Constant", 1)));//car1, 1; car1, 1; car2, 1;

        final String summedByYearAndCarrier = "summedByYearAndCarrier";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.GroupBy,
                Params.of(countedFlights, summedByYearAndCarrier, new Selection(0, 1), new Selection(2), "Sum")));//car1, 2; car2, 1; car3, 2
        final String maxOfFlights = "maxOfFlights";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.GroupBy,
                Params.of(summedByYearAndCarrier, maxOfFlights, new Selection(0), new Selection(2), "Max")));//2
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Join,
                Params.of(maxCarrierInfo, summedByYearAndCarrier, new Selection(2), maxOfFlights, new Selection(1))));//car1, 2; car3, 2;

        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Publish, Params.of(maxCarrierInfo, parsedResult)));
        return steps;
    }



     static ConditionExpression expression(){
        return new ConditionExpression("ColumnPredicate",
                Params.of(UniqueCarrier, new ConditionExpression("Not",
                        Params.of(new ConditionExpression("Equals",
                                Params.of("WN"))))));
    }


    public static void main(String[] args) {
        ;
        try {
            System.setProperty("service.conf", appProperties.getProperty("serviceFile"));
//            System.out.println(System.getProperty("java.class.path"));
//            testDataEngineAlone();
            testDataEngineModelTransform();
//            testCase();
//            testCaseWithJoinOnMap();
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }

}
