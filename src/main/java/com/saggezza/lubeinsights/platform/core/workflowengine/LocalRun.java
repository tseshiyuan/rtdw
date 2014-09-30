package com.saggezza.lubeinsights.platform.core.workflowengine;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRefType;
import com.saggezza.lubeinsights.platform.modules.dataset.DataSetModule;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;
import com.saggezza.lubeinsights.platform.core.common.modules.ModuleFactory;
import com.saggezza.lubeinsights.platform.core.serviceutil.ResourceManager;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by chiyao on 7/24/14.
 */

/**
 * This allows us to run a module locally in workflow engine
 */
public class LocalRun {

    private Statement[] statements;
    private HashMap<String, DataRef> localVars = new HashMap<String, DataRef>();

    public LocalRun(Statement[] statements) {
        this.statements = statements;
    }

    private final void assign(String var, DataRef dataRef) {
        localVars.put(var, dataRef);
    }

    private final DataRef getVal(String var) {
        return localVars.get(var);
    }

    /**
     * command interpreter
     * @param execContext
     * @param inputData
     * @return
     */
    public ExecResult run(ExecContext execContext, String nodeName, DataChannel inputData) {
        DataChannel outputData = new DataChannel();
        for (Statement statement: statements) {

            if (statement.commandName.equalsIgnoreCase("input")) {  // e.g. input bind=x source=f1 (x = input f1)
                // predicate the input
                HashMap<String,String> params = statement.getParams();
                String tag = params.get("source");
                String var = params.get("bind");
                // bind var to the DataRef with tag
                assign(var,inputData.getDataRef(tag));
            }
            else if (statement.commandName.equalsIgnoreCase("output")) {  // e.g. output val=y sink=f2 (output y f2)
                // put to output
                HashMap<String,String> params = statement.getParams();
                String tag = params.get("sink");
                DataRef dataRef = getVal(params.get("val"));
                outputData.putDataRef(tag, dataRef);
             }
            else { // e.g. Y = TestDataSetModule X
                // data set handling logic is defined in modules
                DataSetModule module = (DataSetModule) ModuleFactory.getModule("dataset",statement.commandName);
                HashMap<String,String> params = statement.getParams();

                DataRef inputDataSet = getVal(params.get("val"));
                // create a temp file name
                String varName = params.get("bind");
                String outputAddress = ResourceManager.allocateFile(execContext.getWorkflow().getName()+'.'+nodeName+".localRun."+varName);
                DataRef outputDataSet = new DataRef(DataRefType.FILE,outputAddress); // TODO: always file? (maybe platform app decision)
                // process and output to file
                module.run(execContext.getEnvironment(), inputDataSet, outputDataSet);
                // bind to new variable
                assign(varName,outputDataSet);
            }
        }
        return new ExecResult(ExecStatus.OK, null, outputData);
    }

    public static class Statement {

        protected String commandName;
        protected String[] paramsArray; // name,value,name,value,...

        // build from ArrayList
        public Statement(String commandName, ArrayList<String> paramsArray) {
            this.commandName = commandName;
            this.paramsArray = (String[])paramsArray.toArray();
        }

        public Statement(String commandName, String[] paramsArray) {
            this.commandName = commandName;
            this.paramsArray = paramsArray;
        }

        public HashMap<String,String> getParams() {
            if (paramsArray==null) {
                return null;
            }
            HashMap<String,String> result = new HashMap<String,String>();
            for (int i=0; i<paramsArray.length; i += 2) {
                result.put(paramsArray[i],paramsArray[i+1]);
            }
            return result;
        }

    }


}
