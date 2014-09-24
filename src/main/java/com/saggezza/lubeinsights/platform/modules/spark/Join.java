package com.saggezza.lubeinsights.platform.modules.spark;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.Key;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.Selection;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.DataExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.DataModelExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.ErrorCode;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineMetaSupport;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineModule;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.SparkExecutionContext;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceRequest;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author : Albin
 *
 * Joins two data set based on the key specified.
 *
 * Executed as,
 * <pre>
 *     {@code
 *          execute(Join, outputTag, leftInputTag, rightInputTag, leftKeySelection, rightKeySelection);
 *
 *     }
 * </pre>
 * Join is the command to execute.
 * outputTag is the named output joined dataset.
 * leftInputTag is the name for left input.
 * rightInputTag is the name for the right input.
 * leftKeySelection is the ordered selection of columns as keys for the left input.
 * rightKeySelection is the ordered selection of columns as keys for the right input.
 * Each entry in leftKeySelection is to be logically same as the same indexed entry in the rightKeySelection too.
 */
public class Join implements DataEngineModule, DataEngineMetaSupport{

    public static final Logger logger = Logger.getLogger(Join.class);
    private String outputTag;
    private String fromTag;
    private String toTag;
    private Selection fromKey;
    private Selection toKey;

    public Join(){
    }

    private Join(Params params){
        outputTag = params.get(0);
        fromTag = params.get(1);
        fromKey = params.get(2);
        toTag = params.get(3);
        toKey = params.get(4);
    }

    @Override
    public void execute(ServiceRequest.ServiceStep step, DataExecutionContext context) {
        SparkExecutionContext cont = (SparkExecutionContext) context;
        JavaRDD<DataElement> from = cont.getDataRef(fromTag);
        JavaRDD<DataElement> to = cont.getDataRef(toTag);

        JavaPairRDD<Key, DataElement> keyedFrom = from.<Key>keyBy(
                (DataElement in) -> { return in.key(fromKey); }
        );
        JavaPairRDD<Key, DataElement> keyedTo = to.<Key>keyBy(
                (DataElement in) -> { return in.key(toKey); }
        );
        JavaPairRDD<Key, Tuple2<DataElement, DataElement>> joined = keyedFrom.<DataElement>join(keyedTo);

        logger.debug(String.format("Extracted datasets %s and %s for join", fromTag, toTag));
        JavaRDD<DataElement> map = joined.<DataElement>map((Tuple2<Key, Tuple2<DataElement, DataElement>> in) -> {
            ArrayList<DataElement> added = new ArrayList<>();
            added.addAll(in._2()._1().allValues());
            added.addAll(in._2()._2().allValues());
            return new DataElement(added);
        });
        logger.debug(String.format("Join output written to  %s ", outputTag));
        logger.info(String.format("statement [ %s = join %s and %s ]", outputTag, fromTag, toTag));
        cont.setDataRef(outputTag, map);
    }

    @Override
    public void mockExecute(ServiceRequest.ServiceStep step, DataExecutionContext con) throws DataEngineExecutionException {
        DataModelExecutionContext context = (DataModelExecutionContext) con;
        DataModel from = context.getDataRef(fromTag);
        DataModel to = context.getDataRef(toTag);

        DataModel fromKeyModel = from.select(this.fromKey);
        DataModel toKeyModel = to.select(this.toKey);

        validate(fromKeyModel, toKeyModel);

        ArrayList<DataModel> models = new ArrayList<>();
        models.addAll(fromKeyModel.getList());
        models.addAll(from.allValues());
        models.addAll(to.allValues());

        context.setDataRef(outputTag, new DataModel(models));
    }

    private void validate(DataModel fromKeyModel, DataModel toKeyModel) throws DataEngineExecutionException {
        ArrayList<DataModel> from = fromKeyModel.getList();
        ArrayList<DataModel> to = toKeyModel.getList();
        if(from.size() != to.size()){
            throw new DataEngineExecutionException(ErrorCode.JoinKeySizesDifferent, "Key lengths of join criteria is different");
        }
        for(int i=0; i < from.size(); i++){
            if(from.get(i).getDataType() != to.get(i).getDataType()){
                throw new DataEngineExecutionException(ErrorCode.JoinKeyTypeDifferent, "Key data type for index "+i+
                        " different for join condition");
            }
        }
    }
}
