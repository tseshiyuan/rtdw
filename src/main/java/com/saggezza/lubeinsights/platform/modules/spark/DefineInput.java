package com.saggezza.lubeinsights.platform.modules.spark;

import com.saggezza.lubeinsights.platform.core.common.GsonUtil;
import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRefType;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.DataExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.DataModelExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.ErrorCode;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineMetaSupport;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineModule;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.SparkExecutionContext;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceRequest;

import java.util.Set;

/**
 * @author : Albin
 */
public class DefineInput implements DataEngineModule, DataEngineMetaSupport{

    public DefineInput(){
    }

    private DefineInput(Params params){
    }

    @Override
    public void execute(ServiceRequest.ServiceStep step, DataExecutionContext context) {
        DataChannel channel = GsonUtil.gson().fromJson(
                step.getParams().<String>getFirst(), DataChannel.class);
        context.setDataChannel(channel);
    }

    @Override
    public void mockExecute(ServiceRequest.ServiceStep step, DataExecutionContext context) {
        execute(step, context);//Same as execute
        validateDataChannel(context);
    }

    private void validateDataChannel(DataExecutionContext context) {
        DataChannel dataChannel = context.getDataChannel();
        for(String tag : dataChannel.getTags()){
            if(dataChannel.getDataRef(tag).getDataModel() == null
                || dataChannel.getDataRef(tag).getType() != DataRefType.MODEL){
                throw new RuntimeException("Data Channel should point to data models only for data model transformation. "+tag);
            }
        }
    }


}
