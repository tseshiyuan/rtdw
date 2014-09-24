package com.saggezza.lubeinsights.platform.modules.spark;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElements;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRefType;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.DataExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.DataModelTransformer;
import com.saggezza.lubeinsights.platform.core.dataengine.ErrorCode;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineMetaSupport;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineModule;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.SparkExecutionContext;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceRequest;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceResponse;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;
import java.util.function.Consumer;

/**
 * @author : Albin
 */
public class Browse implements DataEngineModule, DataEngineMetaSupport {

    public static final Logger logger = Logger.getLogger(Filter.class);
    final static String output = "_output";

    public Browse(){
    }

    private Browse(Params params){
    }


    @Override
    public void execute(ServiceRequest.ServiceStep step, DataExecutionContext con) {
        SparkExecutionContext context = (SparkExecutionContext) con;
        DataChannel channel = new DataChannel();

        Params params = step.getParams();
        DataRef key = params.getFirst();
        context.loadFile(output, key);
        List collect = context.getDataRef(output).collect();
        channel.putDataRef(output, new DataRef(DataRefType.VALUE, new DataElements(collect)));
        context.setResponse(new ServiceResponse("OK", "OKAY", channel));
    }

    @Override
    public void mockExecute(ServiceRequest.ServiceStep step, DataExecutionContext context) throws DataEngineExecutionException {
        throw new DataEngineExecutionException(ErrorCode.Stand_Alone_Command,
                "Browse is a stand alone command and does not need data model transformation");
    }
}
