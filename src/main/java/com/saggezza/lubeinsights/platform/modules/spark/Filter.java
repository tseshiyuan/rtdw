package com.saggezza.lubeinsights.platform.modules.spark;

import com.saggezza.lubeinsights.platform.core.common.ConditionExpression;
import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.modules.ModuleException;
import com.saggezza.lubeinsights.platform.core.common.modules.Modules;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.DataExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.DataModelExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.ErrorCode;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineMetaSupport;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineModule;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.SparkExecutionContext;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceRequest;
import org.apache.log4j.Logger;

import java.util.function.Predicate;

/**
 * @author : Albin
 *
 * Filters the input dataset based on the specific predicate.
 * Executed as,
 * <pre>
 *     {@code
 *          execute(Filter, inputTag, outputTag, predicateConditionExpression);
 *
 *     }
 * </pre>
 * Filter is the command.
 * inputTag is the name of the input dataset.
 * outputTag is the name of the output dataset.
 * predicateConditionExpression is the expression by which we would filter the dataset.
 */
public class Filter implements DataEngineModule, DataEngineMetaSupport {

    public static final Logger logger = Logger.getLogger(Filter.class);
    private String input;
    private String output;
    private ConditionExpression expression;
    private Predicate predicate;

    public Filter(){
    }

    private Filter(Params params){
        input = params.getFirst();
        output = params.getSecond();
        expression = ConditionExpression.deserialize(params.getThird());
    }

    @Override
    public void execute(ServiceRequest.ServiceStep step, DataExecutionContext con) throws DataEngineExecutionException {
        SparkExecutionContext context = (SparkExecutionContext) con;
        Params params = step.getParams();
        try {
            predicate = Modules.predicate(expression);
        } catch (ModuleException e) {
            throw new DataEngineExecutionException(ErrorCode.ModuleNotKnown, "Cannot find module for condition expression passed");
        }

        context.setDataRef(output, context.getDataRef(input).filter((Object line) -> {
            return predicate.test(line);
        }));

        logger.info(String.format("statement [ %s = filter %s by predicate ]", output, input));
    }


    @Override
    public void mockExecute(ServiceRequest.ServiceStep step, DataExecutionContext context) throws DataEngineExecutionException {
        ((DataModelExecutionContext)context).copyTheSameModel(input, output);
    }
}
