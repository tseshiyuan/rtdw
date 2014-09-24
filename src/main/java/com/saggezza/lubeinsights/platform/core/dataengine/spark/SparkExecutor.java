package com.saggezza.lubeinsights.platform.core.dataengine.spark;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.modules.ModuleException;
import com.saggezza.lubeinsights.platform.core.dataengine.*;
import com.saggezza.lubeinsights.platform.core.common.modules.Modules;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceConfig;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceRequest;
import org.apache.spark.api.java.JavaRDD;

/**
 * @author : Albin
 */
public class SparkExecutor implements DataEngineExecutor {
    static final String OutputPath = ServiceConfig.load().getSparkOutputFolder();

    @Override
    public void execute(ServiceRequest.ServiceStep step, DataExecutionContext executionContext) throws DataEngineExecutionException {
        SparkExecutionContext context = (SparkExecutionContext) executionContext;
        Modules.module("spark", step.getCommand().name(),
                step.getParams()).execute(step, context);
    }

    @Override
    public String materializeOutput(String outConfig, DataExecutionContext executionContext) {
        SparkExecutionContext context = (SparkExecutionContext) executionContext;
        String outpath = OutputPath;
        if(!outpath.endsWith("/")){
            outpath = outpath + "/";
        }
        String outputPath = outpath + outConfig + System.currentTimeMillis();
        JavaRDD<DataElement> dataRef = context.getDataRef(outConfig);
        JavaRDD<String> toOutput = dataRef.map((DataElement elem) -> {
            return elem.serialize();
        });
        toOutput.saveAsTextFile(outputPath);
        return outputPath;
    }
}
