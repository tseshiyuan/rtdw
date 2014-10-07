package com.saggezza.lubeinsights.platform.core.workflowengine;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.saggezza.lubeinsights.platform.core.common.GsonUtil;
import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRefType;
import com.saggezza.lubeinsights.platform.core.serviceutil.*;
//import com.sun.tools.javac.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.function.Predicate;

/**
 * Created by chiyao on 7/16/14.
 */

/**
 * A WorkFlow object is a spec of jobs with execution order defined in a DAG. Each job is of type NodeWork.
 * A WorkFlow can be instantiated with an initial ExecContext parameter, which represents its run time environment.
 */
public class WorkFlow {

    private String name;
    private HashMap<String,Node> nodes;
    private HashSet<String> links =  new HashSet<String>();

    public WorkFlow(String name, HashSet<Node> nodes) {
        this.name = name;
        this.nodes = new HashMap<String,Node>();
        for (Node node: nodes) {
            this.nodes.put(node.getName(),node);
        }
    }

    /**
     *
     * @return number of nodes in this workflow
     */
    public final int size() {
        return nodes.size();
    }

    public final String getName() {
        return name;
    }

    public void addLink(Node node1, Node node2) {
        links.add(node1.getName()+"->"+node2.getName());
        node2.addPredecessor(node1);
    }

    /**
     * build predecessors for each node in this workflow based on links
     * This is called after deserialization from json representation
     */
    private final void buildNodePredecessorsByLinks() {
        for (String link: links) {
            String[] nodePair = link.split("->");
            nodes.get(nodePair[1]).addPredecessor(nodes.get(nodePair[0]));
        }
    }

    /**
     * @return a linear order of all nodes using topological sort
     */
    public ArrayList<Node> getOrderedNodes() {
        HashSet<Pair> pairs = new HashSet<Pair>();
        HashSet<String> nodesOnLinks = new HashSet<String>();
        for (String link:links) {
            String[] pair = link.split("->");
            pairs.add(new Pair(pair[0],pair[1]));
            nodesOnLinks.add(pair[0]);
            nodesOnLinks.add(pair[1]);
        }
        ArrayList<String> sortedNames = new ArrayList<String>();
        // find nodes not on any link
        for (String name: nodes.keySet()) {
            if (!nodesOnLinks.contains(name)) {
                sortedNames.add(name);
            }
        }
        topologicalSort(sortedNames,pairs);
        // find nodes not on sorted names and add them to sorted names
        for (String name: nodes.keySet()) {
            if (!sortedNames.contains(name)) {
                sortedNames.add(name);
            }
        }
        // convert to name list to node list
        ArrayList<Node> result = new ArrayList<Node>();
        for (String name: sortedNames) {
            result.add(nodes.get(name));
        }
        return result;
    }

    /**
     * recursively remove roots from a DAG and eventually return a list of ordered nodes
     * @param sortedNames   result buffer
     * @param links         set of string pairs
     */
    private void topologicalSort(ArrayList<String> sortedNames,
                                 HashSet<Pair> links) {
        if (links.size()==0) {
            return;
        }
        // find roots (those without incoming links)
        final HashSet<String> lefts = new HashSet<String>();
        HashSet<String> rights = new HashSet<String>();
        for (Pair link: links) {
            lefts.add(link.fst);
            rights.add(link.snd);
        }
        lefts.removeAll(rights); // roots = lefts - rights
        // lefts are the roots
        sortedNames.addAll(lefts);

        links.removeIf(new Predicate<Pair>() {
            @Override
            public boolean test(Pair pair) {
                return (lefts.contains(pair.fst));
            }
        });
        topologicalSort(sortedNames, links);
    }

    /**
     * serialization
     * @return a json string for this workflow
     */
    public String toJson() {
        return GsonUtil.gson().toJson(this);
    }

    public String toJsonPretty() {
        return new GsonBuilder().setPrettyPrinting().create().toJson(this);
    }


    /**
     * deserialization
     * @param json
     * @return a WorkFlow object
     */
    public static WorkFlow fromJson(String json) {
        WorkFlow wf = GsonUtil.gson().fromJson(json, WorkFlow.class);
        wf.buildNodePredecessorsByLinks();
        return wf;
    }

    /**
     *
     * @return a ServiceRequest that contains one command: RUN_WORKFLOW thisWorkFlow inputData
     */
    public final ServiceRequest toServiceRequest(DataChannel inputData) {
        return new ServiceRequest(ServiceCommand.RUN_WORKFLOW,
                Params.of(toJson(), inputData.toJson()));
    }


    /**
     * utility class
     */
    public static class Pair {
        public String fst;
        public String snd;

        public Pair(String s1, String s2) {
            fst = s1;
            snd = s2;
        }
    }

    public static final void main(String[] args) {

        WorkFlowEngine engine = null;
        ServiceGateway gateway = null;

        try {

            // construct a workflow

            DataChannel inputData = new DataChannel();
            inputData.putDataRef("F1", new DataRef(DataRefType.VALUE, "1"));

            // X = input F1
            LocalRun.Statement s1 = new LocalRun.Statement("input", new String[]{"bind", "X", "source", "F1"});
            // Y = SimpleDataSetProcessor X
            LocalRun.Statement s2 = new LocalRun.Statement("SimpleDataSetProcessor", new String[]{"bind", "Y", "val", "X"});
            // output Y F2
            LocalRun.Statement s3 = new LocalRun.Statement("output", new String[]{"val", "Y", "sink", "F2"});
            // X = input F2
            LocalRun.Statement s4 = new LocalRun.Statement("input", new String[]{"bind", "X", "source", "F2"});
            // output Y F3
            LocalRun.Statement s5 = new LocalRun.Statement("output", new String[]{"val", "Y", "sink", "F3"});

            NodeWork work1 = new NodeWork(new LocalRun(new LocalRun.Statement[]{s1, s2, s3}));
            Node node1 = new Node("node1", work1); // F1 -> SimpleDataSetProcessor -> F2
            NodeWork work2 = new NodeWork(new LocalRun(new LocalRun.Statement[]{s4, s2, s5}));
            Node node2 = new Node("node2", work2); // F2 -> SimpleDataSetProcessor -> F3

            WorkFlow workflow1 = new WorkFlow("workflow1", Sets.newHashSet(node1, node2));
            workflow1.addLink(node1, node2);

            // test serialize
            String json = workflow1.toJsonPretty();
            WorkFlow wf = fromJson(json);

            // test embed run (without using service gateway)
            /*
            DataChannel outputData = new WorkFlowEngine().runWorkFlow(workflow1, inputData);
            System.out.println(outputData.toJson());
            */

            // start workflow engine

            engine = new WorkFlowEngine();
            engine.start(8081);

            // request is:  run_workflow workflow1 inputData
            ServiceRequest request = workflow1.toServiceRequest(inputData);

            // send request to work flow engine service
            gateway = ServiceGateway.getServiceGateway();
            ServiceResponse response = gateway.sendRequest(ServiceName.WORKFLOW_ENGINE, request);
            System.out.println("Server response");
            System.out.println(response.getData().toJson());

            engine.stop();
            System.out.println("done");

            System.exit(0);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {

        }

    }

}
