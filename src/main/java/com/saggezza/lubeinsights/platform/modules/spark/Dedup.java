package com.saggezza.lubeinsights.platform.modules.spark;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.DataExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.DataModelExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineMetaSupport;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineModule;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.SparkExecutionContext;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceRequest;
import org.apache.log4j.Logger;

/**
 * @author : Albin
 *
 * Dedups the data set. Similar to a distinct operation.
 */
public class Dedup implements DataEngineModule, DataEngineMetaSupport {

    public static final Logger logger = Logger.getLogger(Dedup.class);

    public Dedup(){
    }

    private Dedup(Params params){
    }

    @Override
    public void execute(ServiceRequest.ServiceStep step, DataExecutionContext con) {
        SparkExecutionContext context = (SparkExecutionContext) con;
        String input = step.getParams().getFirst();
        String output = step.getParams().getSecond();

        context.setDataRef(output, context.getDataRef(input).distinct());
        logger.info(String.format("statement [ %s = dedup on  %s ]", output, input));
    }


    @Override
    public void mockExecute(ServiceRequest.ServiceStep step, DataExecutionContext con) {
        DataModelExecutionContext context = (DataModelExecutionContext) con;
        String input = step.getParams().getFirst();
        String output = step.getParams().getSecond();
        context.copyTheSameModel(input, output);
    }
}
