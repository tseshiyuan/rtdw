package com.saggezza.lubeinsights.platform.modules.spark;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.modules.ModuleException;
import com.saggezza.lubeinsights.platform.core.common.modules.Modules;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.DataExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.DataModelExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.ErrorCode;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineMetaSupport;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineModule;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.SparkExecutionContext;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceRequest;
import org.apache.log4j.Logger;


/**
 * @author : Albin
 *
 * Transforms input data sets based on the specified function.
 * Executed as,
 * <pre>
 *     {@code
 *          execute(Map, inputTag, outputTag, functionSpec);
 *
 *     }
 * </pre>
 * Map is the command.
 * inputTag is the name for the input dataset.
 * outputTag is the name for the output dataset.
 * functionSpec is the transformation to apply.
 */
public class Map implements DataEngineModule, DataEngineMetaSupport {


    public static final Logger logger = Logger.getLogger(Map.class);
    public Map(){
    }

    private Map(Params params){
    }

    @Override
    public void execute(ServiceRequest.ServiceStep step, DataExecutionContext con)
            throws DataEngineExecutionException {
        SparkExecutionContext context = (SparkExecutionContext) con;
        Params params = step.getParams();
        String moduleName = params.getThird();
        try {
            Function function = Modules.function(moduleName, params.remainingFrom(3));

            String second = params.getSecond();
            String first = params.getFirst();
            context.setDataRef(second, context.getDataRef(first).map((Object in) -> {
                return function.apply((DataElement) in);
            }));
            logger.debug(String.format("Map output written to %s ", second));
            logger.info(String.format("statement [ %s = map %s by %s ]", second, first, moduleName));
        }catch (ModuleException e) {
            throw new DataEngineExecutionException(ErrorCode.ModuleNotKnown, e);
        }
    }

    @Override
    public void mockExecute(ServiceRequest.ServiceStep step, DataExecutionContext con) throws DataEngineExecutionException {
        DataModelExecutionContext context = (DataModelExecutionContext) con;
        Params params = step.getParams();
        String moduleName = params.getThird();

        try {
            String second = params.getSecond();
            String first = params.getFirst();

            Function function = Modules.function(moduleName, params.remainingFrom(3));

            DataModel output = function.apply(context.getDataRef(first));
            context.setDataRef(second, output);
        } catch (ModuleException e) {
            throw new DataEngineExecutionException(ErrorCode.ModuleNotKnown, e);
        }
    }
}
