package com.saggezza.lubeinsights.platform.core.dataengine;

import com.saggezza.lubeinsights.platform.core.common.modules.ModuleException;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.SparkExecutionContext;
import com.saggezza.lubeinsights.platform.core.serviceutil.PlatformService;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceName;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceRequest;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceResponse;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.util.Properties;

import static com.saggezza.lubeinsights.platform.core.dataengine.ErrorCode.Data_Model_Cannot_Be_Known;


/**
 * @author : Albin
 */
public class DataEngine extends PlatformService {


    public static final Logger logger = Logger.getLogger(DataEngine.class);
    public static final String DataModel = "dataModel";

    public DataEngine() {
        super(ServiceName.DATA_ENGINE);
    }

    public static void main(String[] args) throws Exception {
        if(args.length > 0){
            // find the engine parameters in config file
            String configFile = System.getProperty("service.conf");
            Properties prop = new Properties();
            prop.load(new FileInputStream(configFile));
            String workflowLocation = prop.getProperty(ServiceName.DATA_ENGINE.name());
            new DataEngine().start(Integer.parseInt(workflowLocation.split(":")[1]));
        }else {
            new DataEngine().start(8082);
        }
    }

    public ServiceResponse processDataModel(ServiceRequest request){
        logger.info("---------------------------------");
        DataExecutionContext context = new DataModelExecutionContext();
        int size = request.getCommandList().size();
        try {
            for (int i = 0; i < size; i++) {
                try {
                    context.executor().execute(request.getCommandList().get(i), context);
                } catch (ModuleException e) {
                    return new ServiceResponse(Data_Model_Cannot_Be_Known.getErrorCode(),
                            "Data model cannot be ");
                }
            }
        }catch (DataEngineExecutionException e){
            logger.error("Error occurred in data engine execution", e);
            return new ServiceResponse(e.getErrorCode(), e.getMessage());
        } catch (ModuleException e) {
            logger.error(e);
            return new ServiceResponse(ErrorCode.ModuleNotKnown.getErrorCode(), "Invalid module");
        } finally {
            context.disconnect();
        }
        logger.info("---------------------------------");
        return context.result();
    }

    @Override
    public ServiceResponse processRequest(ServiceRequest request, String endpoint) {
        if(Default.equals(endpoint)){
            return processData(request);
        }else if(DataModel.equals(endpoint)){
            return processDataModel(request);
        }else {
            throw new RuntimeException("Command not supported");
        }
    }

    private ServiceResponse processData(ServiceRequest request) {
        logger.info("---------------------------------");
        DataExecutionContext context = new SparkExecutionContext();
        int size = request.getCommandList().size();
        try {
            for (int i = 0; i < size; i++) {
                context.executor().execute(request.getCommandList().get(i), context);
            }
        }catch (DataEngineExecutionException e){
            logger.error("Error occurred in data engine execution", e);
            return new ServiceResponse(e.getErrorCode(), e.getMessage());
        } catch (ModuleException e) {
            logger.error(e);
            return new ServiceResponse(ErrorCode.ModuleNotKnown.getErrorCode(), "Invalid module");
        } catch (Exception e){
            logger.error(e);
            throw new RuntimeException(e);
        } finally {
            context.disconnect();
        }
        logger.info("---------------------------------");
        return context.result();
    }


}
