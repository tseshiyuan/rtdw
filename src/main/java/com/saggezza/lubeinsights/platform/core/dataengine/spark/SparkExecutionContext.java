package com.saggezza.lubeinsights.platform.core.dataengine.spark;

import com.saggezza.lubeinsights.platform.core.common.Utils;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutor;
import com.saggezza.lubeinsights.platform.core.dataengine.DataExecutionContext;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceConfig;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceResponse;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.HashMap;
import java.util.Map;

/**
 * @author : Albin
 */
public class SparkExecutionContext extends DataExecutionContext {

    public static final Logger logger = Logger.getLogger(SparkExecutionContext.class);

    private final JavaSparkContext context;
    private final Map<String, JavaRDD> dataRefs;
    private DataChannel dataChannel;
    private final SparkExecutor sparkExecutor;
    private ServiceResponse response;

    public SparkExecutionContext() {
        this.context = context(ServiceConfig.load().getSparkMaster());
        this.sparkExecutor = new SparkExecutor();
        this.dataRefs = new HashMap<>();
    }

    @Override
    public DataChannel getDataChannel() {
        return dataChannel;
    }

    @Override
    public void setDataChannel(DataChannel dataChannel) {
        this.dataChannel = dataChannel;
    }

    @Override
    public JavaRDD getDataRef(String tagName) {
        return dataRefs.get(tagName);
    }

    @Override
    public void setDataRef(String name, Object dataRef) {
        dataRefs.put(name, (JavaRDD) dataRef);
    }

    @Override
    public DataEngineExecutor executor() {
        return sparkExecutor;
    }

    @Override
    public void disconnect() {
        context.stop();
    }

    public void setResponse(ServiceResponse response){
        this.response = response;
    }

    @Override
    public ServiceResponse result() {
        return response;
    }

    public void loadFile(String tag, DataRef dataRef){
        String filePath = dataRef.getFileName();
        filePath = filePath.replace("spark:/","");//Loading from local file for now.
        JavaRDD inRdd = context.textFile(filePath);

        if(filePath.startsWith(SparkExecutor.OutputPath)){
            logger.debug("Parsing temporary file input "+filePath);
            inRdd = inRdd.map((Object in) -> {
                String inText = (String) in;
                return DataElement.parseSerialized(inText);
            });
        }else{
            inRdd = inRdd.map((Object in) -> {
                String inText = (String)in;
                return new DataElement(DataType.TEXT, inText);
            });
        }
        dataRefs.put(tag, inRdd);
    }

    private static JavaSparkContext context(String master){
        SparkConf simpleAPP = new SparkConf().setAppName("DataEngineApp "+ Utils.currentTime()).
                setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(simpleAPP);
        return sc;
    }

}
