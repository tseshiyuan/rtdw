package com.saggezza.lubeinsights.platform.core.workflowengine;


/**
 * Created by chiyao on 7/16/14.
 */

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;
import com.saggezza.lubeinsights.platform.core.serviceutil.*;
import com.saggezza.lubeinsights.platform.core.serviceutil.PlatformService;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * workflow engine runs workflows and manages their execution
 * It currently accepts only one command: "run workflow"
 */
public class WorkFlowEngine extends PlatformService {

    public static final Logger logger = Logger.getLogger(WorkFlowEngine.class);

    private ConcurrentHashMap<String,ExecContext> allRunningContexts = new ConcurrentHashMap<String,ExecContext>();

    public WorkFlowEngine() {
        super(ServiceName.WORKFLOW_ENGINE);
    }

    /**
     * process a request (of running a workflow)
     * @param request
     * @param command
     * @return an OK ServiceResponse, whose data field is a DataChannel. Or an ERROR ServiceResponse
     * (A DataChannel is a map from identifier to DataRef (like a value, file or table)
     */
    public ServiceResponse processRequest(ServiceRequest request, String command) {

        try {

            logger.info("WorkflowEngine processes request:\n" + request.toJson());

            if (request == null) {
                logger.error("request is null");
            }

            ArrayList<ServiceRequest.ServiceStep> steps = request.getCommandList();

            if (steps == null || steps.size() != 1) {
                return new ServiceResponse("ERROR", "Bad Request for WorkFlowEngine. Only one command is allowed in request", null);
            }

            ServiceRequest.ServiceStep step = steps.get(0);

            //System.out.println(step.getCommand());
            //System.out.println(step.getParams().getFirst().toString());
            //System.out.println(step.getParams().getSecond().toString());

            // command interpreter
            ServiceResponse response = null;
            switch (step.getCommand()) {
                case RUN_WORKFLOW:
                    // parse params
                    WorkFlow workflow = WorkFlow.fromJson(step.getParams().get(0));
                    DataChannel inputData = DataChannel.fromJson(step.getParams().get(1));
                    // run workflow
                    DataChannel outputData = runWorkFlow(workflow, inputData);
                    // build response
                    response = new ServiceResponse("OK", null, outputData); // ONLY take 1 command for no
                    break;
                default:
                    // only support one command now
                    response = new ServiceResponse("ERROR", "Bad command for WorkFlowEngine. Only RUN_WORKFLOW is valid", null);
            }
            logger.info("WorkflowEngine's response:\n" + response.toJson());
            return response;
        } catch (Exception e) {
            logger.trace("WorkFlowEngine Error", e);
            return new ServiceResponse("ERROR", e.getMessage(), null);
        }

    }


    /**
     * run a workflow, when it is not running
     * @param workflow
     * @return true iff workflow is triggered successfully
     */
    public DataChannel runWorkFlow(WorkFlow workflow, DataChannel inputData) {
        try {
            if (isRunning(workflow.getName())) {
                logger.warn("Can't start a workflow while it's running.");
                return null;
            }
            ExecContext context = new ExecContext(workflow);
            allRunningContexts.put(workflow.getName(),context); // track this run
            ArrayList<Node> nodes = workflow.getOrderedNodes();
            // launch all nodes in order
            for (Node node : nodes) {
                node.run(context, inputData); // every node can take workflow input dataaccess
            }
            // collect output from all nodes as workflow output
            DataChannel outputData = new DataChannel();
            for (Node node: nodes) {
                outputData.merge(context.getNodeExecResult(node).getValue()); // blocking call
            }
            context.shutDownExecutor();
            allRunningContexts.remove(workflow.getName()); // remove this workflow from allRunningContexts
            return outputData;
        } catch (Exception e) {
            logger.error("WorkFlowEngine Error for workflow "+workflow.getName());
            return null;
        }
    }

    /**
     *
     * @param workflowName
     * @return true iff the workflow is running
     */
    public boolean isRunning(String workflowName) {
        ExecContext context = allRunningContexts.get(workflowName);
        if (context==null) {
            return false;
        }
        return !context.isFinished();
    }

    public static void main(String[] args) throws Exception {
        try {
            //System.out.println(Arrays.toString(args));
            if (args.length > 0) {
                // find the engine parameters in config file
                String configFile = System.getProperty("service.conf");
                Properties prop = new Properties();
                prop.load(new FileInputStream(configFile));
                String workflowLocation = prop.getProperty(ServiceName.WORKFLOW_ENGINE.name());
                new WorkFlowEngine().start(Integer.parseInt(workflowLocation.split(":")[1]));
            } else {
                new WorkFlowEngine().start(8081);
            }
            logger.warn("WorkflowEngine started");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
