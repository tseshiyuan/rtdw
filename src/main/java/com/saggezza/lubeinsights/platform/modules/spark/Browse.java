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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author : Albin
 */
public class Browse implements DataEngineModule, DataEngineMetaSupport {

    public static final Logger logger = Logger.getLogger(Filter.class);
    final static String output = "_output";
    private int pageSize = 20;
    private int pageNum = 1;

    public Browse(){
    }

    private Browse(Params params){
        if(params.size() > 1){
            this.pageSize = params.getSecond();
        }
        if(params.size() > 2){
            this.pageNum = params.getThird();
        }
    }


    @Override
    public void execute(ServiceRequest.ServiceStep step, DataExecutionContext con) {
        SparkExecutionContext context = (SparkExecutionContext) con;
        DataChannel channel = new DataChannel();

        Params params = step.getParams();
        DataRef key = params.getFirst();
        context.loadFile(output, key);
        JavaRDD outputRDD = context.getDataRef(output);
        Iterator rddIter = outputRDD.toLocalIterator();

        channel.putDataRef(output, new DataRef(DataRefType.VALUE, new DataElements(getPage(rddIter))));
        context.setResponse(new ServiceResponse("OK", "OKAY", channel));
    }

    private List getPage(Iterator iterator){
        List list = new ArrayList<>();
        for(int i=1; i < pageNum; i++){//All previous pages
            for(int j=1; j <= pageSize && iterator.hasNext(); j++){
                iterator.next();//Traversing
            }
        }
        //Current page
        for(int j=1; j <= pageSize && iterator.hasNext(); j++){
            list.add(iterator.next());
        }
        return list;
    }

    @Override
    public void mockExecute(ServiceRequest.ServiceStep step, DataExecutionContext context) throws DataEngineExecutionException {
        throw new DataEngineExecutionException(ErrorCode.Stand_Alone_Command,
                "Browse is a stand alone command and does not need data model transformation");
    }
}
