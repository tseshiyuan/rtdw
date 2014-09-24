package com.saggezza.lubeinsights.platform.core.workflowengine;

import com.saggezza.lubeinsights.platform.core.common.Environment;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceConfig;
import org.apache.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by chiyao on 7/15/14.
 */

/**
 * ExecContext tracks the execution status of a workflow.
 */
public class ExecContext extends ConcurrentHashMap {

    public static final Logger logger = Logger.getLogger(ExecContext.class);

    private static final int THREAD_COUNT = 10;
    private ConcurrentHashMap<Node,Future<ExecResult>> runningNodes = new ConcurrentHashMap<Node,Future<ExecResult>>();
    private ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    private ServiceConfig config = ServiceConfig.load();
    private WorkFlow workflow;
    private Environment environment;
    private AtomicInteger completionCount = new AtomicInteger(0);


    public ExecContext(WorkFlow workflow) {

        this.workflow = workflow;
        environment = new Environment(config.get("tenant"), config.get("application"), workflow.getName()); // use workflow name as environment name
    }

    /**
     * get a node's execution result, blocking if not complete
     * @param node
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws WorkFlowException
     */
    public final ExecResult getNodeExecResult(Node node) throws InterruptedException, ExecutionException, WorkFlowException {
        Future<ExecResult> future = runningNodes.get(node);
        if (future != null) {
            return future.get(); // throws ExecutionException if computation throws Exception
        }
        else {
            throw new WorkFlowException("Workflow node not running");
        }
    }

    public void addNodeExec(Node node, Future<ExecResult> future) {
        runningNodes.put(node,future);
    }

    public final ExecutorService getExecutor() {
        return executor;
    }

    public final boolean isFinished() {
        for (Future<ExecResult> future: runningNodes.values()) {
            if (!future.isDone()) {
                return false;
            }
        }
        return true;
    }

    public int getNumberOfRunningNodes() {
        return runningNodes.size() - completionCount.get();
    }

    public void reportCompletion(String nodeName) {
        logger.info("Node " +nodeName + " is complete for workflow "+workflow.getName());
        completionCount.addAndGet(1);
    }

    public void shutDownExecutor() {
        logger.info("Shutting down executor for workflow "+workflow.getName());
        System.out.println("Shutting down executor for workflow "+workflow.getName());
        executor.shutdown();
    }

    public final Environment getEnvironment() {return environment;}
    public final WorkFlow getWorkflow() {return workflow;}

}
