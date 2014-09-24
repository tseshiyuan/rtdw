package com.saggezza.lubeinsights.platform.core.workflowengine;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;
import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.serviceutil.*;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Created by chiyao on 7/15/14.
 */

/**
 * The node work is either a service request or a local execution of a module
 */
public class NodeWork {

    public static final Logger logger = WorkFlowEngine.logger;  // log to workflow engine log

    // node work is either a service call
    protected ServiceName serviceName=null;
    protected ServiceRequest serviceRequest=null;
    // or to run a module
    protected LocalRun localRun=null;

    public NodeWork(ServiceName serviceName, ServiceRequest serviceRequest) {
        this.serviceName = serviceName;
        this.serviceRequest = serviceRequest;
    }

    public NodeWork(LocalRun localRun) {
        this.localRun = localRun;
    }

    public ExecResult run(ExecContext context, String nodeName, DataChannel inputData)
        throws InterruptedException, TimeoutException, ExecutionException, Exception {

        if (localRun != null) {  // it's a local run
            return localRun.run(context, nodeName, inputData);  // using context to pass states across nodes
        }
        else {  // it's a service call
            // add 1st step: define_input so that we can pass on inputData
            ServiceRequest newRequest = ServiceRequest.createServiceRequest(
                    ServiceCommand.DefineInput,
                    Params.of(inputData.toJson()),
                    serviceRequest);
            String msg = String.format("WorkflowEngine sends service request to DataEngine: (workflow %s node %s) \n %s",
                    nodeName, context.getWorkflow().getName(), newRequest);
            logger.info(msg);
            ServiceResponse serviceResponse = ServiceGateway.getServiceGateway().sendRequest(serviceName, newRequest);
            msg = String.format("WorkflowEngine receives response from DataEngine: (workflow %s node %s) \n %s",
                    nodeName, context.getWorkflow().getName(), newRequest);
            logger.info(msg);
            // get result value from service response
            return new ExecResult(ExecStatus.fromStatusNode(serviceResponse.getStatus()),
                                  serviceResponse.getMessage(),
                                  serviceResponse.getData());
        }
        // let caller handle exception
    };

}

