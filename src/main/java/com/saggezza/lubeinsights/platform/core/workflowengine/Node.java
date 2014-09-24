package com.saggezza.lubeinsights.platform.core.workflowengine;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceName;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.concurrent.*;


/**
 * Created by chiyao on 7/15/14.
 */
public class Node {

    public static final Logger logger = Logger.getLogger(Node.class);

    private transient HashSet<Node> predecessors = null;  // don't serialize/persist this field as links are represented in workflow
    private NodeWork work;
    private String name; // must be unique in a workflow

    public Node(String name, NodeWork work) {
        this.name = name;
        this.work = work;
    }

    public final String getName() {
        return name;
    }

    /**
     * identify node by name
     * @return true iff name equals
     */
     public boolean equals(Node node) {
        return name.equals(node.name);
    }

    public final int hashCode() {
        return name.hashCode();
    }  // identify node by name

    public final void addPredecessor(Node node) {
        if (predecessors == null) {
            predecessors = new HashSet<Node>();
        }
        predecessors.add(node);
    }

    /**
     * execute the logic for this node asynchronously under the workflow context
     * All exceptions are logged and handled here
     * @param context    the environment for this workflow instantiation
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws WorkFlowException
     */
    public Future<ExecResult> run(final ExecContext context, final DataChannel workflowInput)  {
        ExecutorService executor = context.getExecutor();
        final String myName = this.name;
        Callable<ExecResult> callable = new Callable<ExecResult>() {
            public ExecResult call() {
                //test
                System.out.println("WorkflowEngine in node "+myName + " of workflow "+context.getWorkflow().getName());

                try {

                    DataChannel inputData = new DataChannel();
                    inputData.merge(workflowInput);
                    if (predecessors != null) {
                        for (Node node : predecessors) {
                            ExecResult result = null;
                            try {
                                result = context.getNodeExecResult(node);
                            } catch (Exception e) {
                                logger.warn("error from node " + node.getName());
                            }
                            if (result == null || !result.isOK()) {
                                return new ExecResult(ExecStatus.NOT_RUN,
                                        "Not run due to incomplete predecessor " + node.getName(),
                                        null);
                            } else {
                                DataChannel channel = result.getValue();
                                // build my read channel based on predecessor's output channel
                                inputData.merge(channel);
                            }
                        }
                    }
                    return work.run(context,name, inputData);

                } catch (Exception e) {

                    e.printStackTrace();
                    logger.trace("NodeWork execution error", e);
                    return new ExecResult(ExecStatus.ERROR,
                                          "NodeWork execution error " + myName,
                                          null);
                } finally {
                    // report completion
                    context.reportCompletion(myName);
                }

            }
        };
        Future<ExecResult> future = executor.submit(callable);
        context.addNodeExec(this,future);
        return future;
    }



    public static final void main(String[] args) {
        try {
            ExecutorService executor = Executors.newFixedThreadPool(10);
            Callable<String> callable = new Callable<String>() {
                public String call() {
                    try {
                        return "callable result";
                    } catch (Exception e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            };
            Future<String> future = executor.submit(callable);
            System.out.println(future.isDone());
            System.out.println(future.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
