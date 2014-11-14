package com.saggezza.lubeinsights.platform.modules.spark;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRefType;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.DataExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.DataModelExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineMetaSupport;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineModule;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.SparkExecutionContext;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceRequest;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceResponse;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * @author : Albin
 */
public class Publish implements DataEngineModule, DataEngineMetaSupport {

    public static final Logger logger = Logger.getLogger(Publish.class);

    public Publish(){
    }
    private Publish(Params params){
    }

    @Override
    public void execute(ServiceRequest.ServiceStep step, DataExecutionContext con) {
        SparkExecutionContext context = (SparkExecutionContext) con;
        List<String> tags = step.getParams().asList();
        DataChannel out = context.result() == null ? new DataChannel() : context.result().getData();
        for(String outConfig : tags){
            out.putDataRef(outConfig, new DataRef(DataRefType.FILE,
                    con.executor().materializeOutput(outConfig, con) ));
        }
        logger.info("statement [ publish "+tags + " ]");
        context.setResponse(new ServiceResponse("OK","OKAY",out));
    }

    @Override
    public void mockExecute(ServiceRequest.ServiceStep step, DataExecutionContext con) throws DataEngineExecutionException {
        DataModelExecutionContext context = (DataModelExecutionContext) con;
        DataChannel out = new DataChannel();
        List<String> tags = step.getParams().asList();
        for(String each : tags){
            out.putDataRef(each, new DataRef(DataRefType.FILE, "sample", context.getDataRef(each)));
        }
        context.setDataChannel(out);
    }
}
