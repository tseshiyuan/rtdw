package com.saggezza.lubeinsights.platform.modules.spark;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.DataExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.DataModelExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.ErrorCode;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineMetaSupport;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineModule;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.SparkExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.SparkExecutor;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceRequest;
import org.apache.log4j.Logger;

import java.util.List;

import static com.saggezza.lubeinsights.platform.core.dataengine.ErrorCode.Input_Data_Model_Not_Present;

/**
 * @author : Albin
 */
public class Input implements DataEngineModule, DataEngineMetaSupport {

    public static final Logger logger = Logger.getLogger(Input.class);

    public Input(){
    }
    private Input(Params params){
    }


    @Override
    public void execute(ServiceRequest.ServiceStep step, DataExecutionContext con) {
        SparkExecutionContext context = (SparkExecutionContext) con;
        DataChannel dataChannel = context.getDataChannel();
        List<String> tags = step.getParams().asList();
        logger.info("statement [ input "+tags+" ]");
        SparkExecutor executor = (SparkExecutor) context.executor();

        for(String eachTag : tags){
            context.loadFile(eachTag, dataChannel.getDataRef(eachTag));
        }
    }

    @Override
    public void mockExecute(ServiceRequest.ServiceStep step, DataExecutionContext con) throws DataEngineExecutionException {
        List<String> tags = step.getParams().asList();
        DataModelExecutionContext context = (DataModelExecutionContext) con;

        DataChannel dataChannel = context.getDataChannel();
        for(String eachTag : tags){
            DataRef dataRef = dataChannel.getDataRef(eachTag);
            if(dataRef.getDataModel() == null){
                throw new DataEngineExecutionException(Input_Data_Model_Not_Present,
                        "Data model is not provided for input tag "+eachTag);
            }
            context.setDataRef(eachTag, dataRef.getDataModel());
        }
    }

}
