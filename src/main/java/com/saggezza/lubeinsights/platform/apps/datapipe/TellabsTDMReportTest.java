package com.saggezza.lubeinsights.platform.apps.datapipe;

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
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

import static com.google.common.collect.Sets.newHashSet;

/**
 * Created by venkateswararao on 25/9/14.
 */
public class TellabsTDMReportTest {

    static final String tdmFile = "tdm";
    static final String nodeReportFile = "node_report";
    static final String tdmNodeJoinOnNodeId = "rncJoinOnsmb";
    static final String tdm24HourAvgDataStep = "tdm24HourAvgDataStep";

    static Properties appProperties = new Properties();

    static {
        try {
            appProperties.load(TellabsTDMReportTest.class.getResourceAsStream("/app.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void testCase() throws Exception {
        WorkFlowEngine workFlowEngine = new WorkFlowEngine();
        workFlowEngine.start(8081);
        DataEngine engine = new DataEngine();
        engine.start(8082);

        NodeWork work1 = new NodeWork(ServiceName.DATA_ENGINE, new ServiceRequest(serviceSteps(false)));
        /*NodeWork work2 = new NodeWork(ServiceName.DATA_ENGINE, new ServiceRequest(serviceSteps2(false)));*/

        Node node1 = new Node("Tellabs test Node1", work1);
        /*Node node2 = new Node("Tellabs test Node1", work2);*/

        HashSet<Node> nodes = newHashSet(node1);

        WorkFlow workFlow = new WorkFlow("Tellabs test work", nodes);
        /*workFlow.addLink(node1,node2);*/

        ServiceGateway gateway = ServiceGateway.getServiceGateway();
        ServiceResponse serviceResponse = gateway.sendRequest(ServiceName.WORKFLOW_ENGINE,
                workFlow.toServiceRequest(inChannel()));

        DataRef dataRef = null;
        for (String each : serviceResponse.getData().getTags()) {
            System.out.println(each);
            System.out.println(serviceResponse.getData().getDataRef(each).getFileName());
            dataRef = serviceResponse.getData().getDataRef(each);
            System.out.println("---------------------------------------------------");
        }

        ServiceResponse collectionResponse = gateway.sendRequest(
                ServiceName.DATA_ENGINE, new ServiceRequest(ServiceCommand.Browse, Params.of(dataRef)));
        DataRef singleDataRef = collectionResponse.getData().getSingleDataRef();
        List<DataElement> data = singleDataRef.getValue();
        for (DataElement element : data) {
            System.out.println(element.toString());
        }

        workFlowEngine.stop();
        engine.stop();

    }

    private static DataChannel inChannel() {
        DataChannel in = new DataChannel();
        in.putDataRef(tdmFile, new DataRef(DataRefType.FILE,
                "spark:/" + appProperties.getProperty("tdm"), new DataModel(DataType.TEXT)));
        in.putDataRef(nodeReportFile, new DataRef(DataRefType.FILE,
                "spark:/" + appProperties.getProperty("node_report"), new DataModel(DataType.TEXT)));
        return in;
    }

    private static ArrayList<ServiceRequest.ServiceStep> serviceSteps(boolean standAlone) {
        ArrayList<ServiceRequest.ServiceStep> steps = new ArrayList<>();

        if (standAlone) {
            DataChannel in = inChannel();
            steps.add(new ServiceRequest.ServiceStep(ServiceCommand.DefineInput, Params.of(in.toJson())));
        }

        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Load, Params.of(tdmFile)));
        final String filteredTDMData = "filteredTDMData";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Filter, Params.of(tdmFile, filteredTDMData, preFilterCondition().toJson())));
        final String filterTDMHeaderData = "filterTDMHeaderData";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Filter, Params.of(filteredTDMData, filterTDMHeaderData, filterCondition2().toJson())));
        final String parsedTDMResult = "parsedTDMResult";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(filterTDMHeaderData, parsedTDMResult, "CsvParse", ";", 46)));
        final String postFilterTDMHeaderData = "filterTDMHeaderData";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Filter, Params.of(parsedTDMResult, postFilterTDMHeaderData, postFilterCondition().toJson())));
        final String selectedTDMResult = "selectedTDMResult";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(postFilterTDMHeaderData, selectedTDMResult, "Select",
                new Selection(0, 3, 22, 27, 28, 35, 36, 37, 40, 41, 42, 43, 44, 45, 46, 6, 9, 2))));

        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Load, Params.of(nodeReportFile)));
        final String filteredNodeData = "filteredNodeData";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Filter, Params.of(nodeReportFile, filteredNodeData, preFilterCondition().toJson())));
        final String filterNodeHeaderData = "filterNodeHeaderData";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Filter, Params.of(filteredNodeData, filterNodeHeaderData, filterCondition3().toJson())));
        final String parsedNodeResult = "parsedNodeResult";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(filterNodeHeaderData, parsedNodeResult, "CsvParse", ";", 8)));
        final String selectedNodeResult = "selectedNodeResult";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(parsedNodeResult, selectedNodeResult, "Select",
                new Selection(0, 5))));


        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Join,
                Params.of(tdmNodeJoinOnNodeId, selectedTDMResult, new Selection(17), selectedNodeResult, new Selection(0))));

        final String subStringDate = "subStringDate";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(tdmNodeJoinOnNodeId, subStringDate, "ColumnTransform", 3, "InvokerTransform",
                "substring", 0, 10)));

        final String summedTDM = "summedTDM";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.GroupBy,
                Params.of(subStringDate, summedTDM, new Selection(1, 2, 3), new Selection(5, 6, 7, 8, 9, 10, 11, 12, 13, 14), "Sum", "Sum", "Sum", "Sum", "Sum", "Sum", "Sum", "Sum", "Sum", "Sum")));

        final String replaceString = "replaceString";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(summedTDM, replaceString, "ColumnTransform", 2, "SearchAndReplace",
                "-", "")));

        /*final String mergeColumns = "mergeColumns";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(replaceString, mergeColumns, "Merge", new Selection(2, 0, 1), ":")));

        final String trimMergedColumn = "trimMergedColumn";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(mergeColumns, trimMergedColumn, "ColumnTransform", 13, "TextTrim")));
*/
        String script = "var resultValue = new java.lang.StringBuilder(); var dataElements = dataElement.allValues();  var delimiter = ':'; " +
                "for (i in dataElements) { resultValue.append(dataElements[i].value()).append(delimiter);}" +
                "dataElement.asList().add(new DataElement(DataType.TEXT, StringUtils.isEmpty(delimiter) ? resultValue.toString():" +
                "resultValue.toString().substring(0, resultValue.length() - 1)));";
        List<String> userDefinedBeans = Arrays.asList("org.apache.commons.lang.StringUtils");

        final String scriptEngine = "scriptEngine";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(replaceString, scriptEngine, "ScriptExecutor", script,Boolean.TRUE, new Selection(2, 0, 1), userDefinedBeans)));
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Publish, Params.of(scriptEngine)));
        return steps;
    }

    private static ConditionExpression preFilterCondition() {
        ConditionExpression contains = new ConditionExpression("Contains", Params.of("#"));
        ConditionExpression notContains = new ConditionExpression("Not", Params.of(contains));
        ConditionExpression regex = new ConditionExpression("Regex", Params.of("^\\s*$"));
        ConditionExpression notRegex = new ConditionExpression("Not", Params.of(regex));
        ConditionExpression andCondition = new ConditionExpression("And", Params.of(notContains, notRegex));
        return andCondition;
    }

    private static ConditionExpression postFilterCondition() {
        ConditionExpression columnValueCheck = new ConditionExpression("ColumnPredicate",
                Params.of(21, new ConditionExpression("Not",
                        Params.of(new ConditionExpression("Equals",
                                Params.of("NoPWs"))))));
        ConditionExpression columnValueCheck1 = new ConditionExpression("ColumnPredicate",
                Params.of(21, new ConditionExpression("Not",
                        Params.of(new ConditionExpression("Equals",
                                Params.of("Affects"))))));
        ConditionExpression andCondition = new ConditionExpression("And", Params.of(columnValueCheck, columnValueCheck1));
        return andCondition;
    }

    private static ConditionExpression filterCondition2() {
        ConditionExpression containsName = new ConditionExpression("Contains", Params.of("pmtype"));
        ConditionExpression notContainsName = new ConditionExpression("Not", Params.of(containsName));
        return notContainsName;
    }

    private static ConditionExpression filterCondition3() {
        ConditionExpression containsName = new ConditionExpression("Contains", Params.of("Node"));
        ConditionExpression notContainsName = new ConditionExpression("Not", Params.of(containsName));
        return notContainsName;
    }

    private static ArrayList<ServiceRequest.ServiceStep> serviceSteps2(boolean standAlone) {
        ArrayList<ServiceRequest.ServiceStep> steps = new ArrayList<>();

        if (standAlone) {
            DataChannel in = inChannel();
            steps.add(new ServiceRequest.ServiceStep(ServiceCommand.DefineInput, Params.of(in.toJson())));
        }
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Load, Params.of(tdmNodeJoinOnNodeId)));
        final String subStringDate = "subStringDate";
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Map, Params.of(tdmNodeJoinOnNodeId, subStringDate, "ColumnTransform", 3, "InvokerTransform",
                "substring", 0, 13)));

        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.GroupBy,
                Params.of(subStringDate, tdm24HourAvgDataStep, new Selection(1, 2, 3), new Selection(6, 7, 8, 9, 10, 11, 12, 13, 14,15), "Sum", "Sum", "Sum", "Sum", "Sum", "Sum", "Sum", "Sum", "Sum", "Sum")));
        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Publish, Params.of(tdm24HourAvgDataStep)));
        return steps;
    }

    public static void main(String[] args) {
        try {
            System.setProperty("service.conf", appProperties.getProperty("serviceFile"));
            testCase();
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }

}
