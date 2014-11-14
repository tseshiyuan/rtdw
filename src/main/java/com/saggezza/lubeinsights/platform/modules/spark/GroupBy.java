package com.saggezza.lubeinsights.platform.modules.spark;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.Key;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.Selection;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.common.modules.Modules;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.DataExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.DataModelExecutionContext;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Aggregator;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineMetaSupport;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.DataEngineModule;
import com.saggezza.lubeinsights.platform.core.dataengine.spark.SparkExecutionContext;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceRequest;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.ArrayList;

/**
 * @author : Albin
 *         <p>
 *         Groups by a specific key on the incoming dataset.
 *         Called as,
 *         <pre>
 *                                             {@code
 *                                                  execute(GroupBy, inputDataSet, outputDataSet, keySelection, aggregationSelection, aggregationOperations);
 *                                             }
 *                                         </pre>
 *         GroupBy is the command.
 *         inputDataSet is the tag/name for the input dataset.
 *         outputDataSet is the tag/name for the output dataset.
 *         keySelection is selection any number of columns from the data set as the key.
 *         aggregationSelection is selection of any number of columns from the dataset to apply aggregation on.
 *         aggregationOperations is the ordered list of aggregations to be performed on the selected columns.
 */
public class GroupBy implements DataEngineModule, DataEngineMetaSupport {

    public static final Logger logger = Logger.getLogger(GroupBy.class);

    public GroupBy() {
    }

    private GroupBy(Params params) {
    }

    @Override
    public void execute(ServiceRequest.ServiceStep step, DataExecutionContext con) {
        SparkExecutionContext context = (SparkExecutionContext) con;
        Params params = step.getParams();
        String inputTag = params.get(0);
        String outputTag = params.get(1);
        Selection groupBykeySpec = params.get(2);

        Selection applyAggregator = params.size() > 3 ? params.get(3) : Selection.Empty;
        Params groupByOperations = params.size() > 4 ? params.remainingFrom(4) : Params.None;

        logger.debug(String.format("Performing group by of %s ", inputTag));
        JavaRDD<DataElement> input = context.getDataRef(inputTag);

        JavaRDD<DataElement> arrayBasedSubset = subsetSelection(groupBykeySpec, applyAggregator, input);
        logger.debug("Subsetted the input ");
        //TODO - 1
        JavaPairRDD<Key, DataElement> keyRecordJavaPairRDD = arrayBasedSubset.<Key>keyBy((DataElement r) -> {
            return r.key(Selection.Range(0, groupBykeySpec.length()));
        });
        logger.debug("Split into key and value");

        JavaPairRDD<Key, DataElement> reducedResult = keyRecordJavaPairRDD.reduceByKey((DataElement r1, DataElement r2) -> {
            for (int i = groupBykeySpec.length(), operIndex = 0; i < r1.length(); i++, operIndex++) {
                String aggregatorName = groupByOperations.get(operIndex);
                Aggregator aggregator = Modules.aggregator(aggregatorName);
                // auto conversion
                double result1 = 0, result2 = 0;
                String value1 = r1.valueAt(i).value().toString();
                String value2 = r2.valueAt(i).value().toString();
                if (StringUtils.isNotEmpty(value1)) {
                    result1 = Double.parseDouble(value1);
                }
                if (StringUtils.isNotEmpty(value2)) {
                    result2 = Double.parseDouble(value2);
                }
                r1.setValueAt(i,
                        new DataElement(DataType.NUMBER,
                                aggregator.aggregate(result1, result2)));
            }
            return r1;
        });
        logger.debug("Grouped and reduced");
        JavaRDD<DataElement> result = reducedResult.<DataElement>map((Tuple2<Key, DataElement> tuple) -> {
            return tuple._2();
        });
        logger.debug(String.format("Grouped output written to %s ", outputTag));
        logger.info(String.format("statement [ %s = group by %s apply %s ]", outputTag, inputTag, groupByOperations.asList().toString()));

        context.setDataRef(outputTag, result);
    }

    private JavaRDD<DataElement> subsetSelection(Selection groupBykeySpec, Selection applyAggregator, JavaRDD<DataElement> input) {
        return input.<DataElement>map((DataElement r) -> {
            ArrayList<DataElement> selected = new ArrayList();
            DataElement allKeys = r.select(groupBykeySpec);
            selected.addAll(allKeys.allValues());
            DataElement aggregationColumns = r.select(applyAggregator);
            selected.addAll(aggregationColumns.allValues());
            return new DataElement(selected);
        });
    }

    @Override
    public void mockExecute(ServiceRequest.ServiceStep step, DataExecutionContext con) throws DataEngineExecutionException {
        DataModelExecutionContext context = (DataModelExecutionContext) con;
        Params params = step.getParams();
        String inputTag = params.get(0);
        String outputTag = params.get(1);
        Selection groupBykeySpec = params.get(2);
        Selection applyAggregator = params.get(3);
        Params groupByOperations = params.remainingFrom(4);

        ArrayList<DataModel> models = new ArrayList<>();
        DataModel input = context.getDataRef(inputTag);
        models.addAll(input.select(groupBykeySpec).getList());
        for (int i = 0; i < applyAggregator.length(); i++) {
            models.add(new DataModel(DataType.NUMBER));
        }

        context.setDataRef(outputTag, new DataModel(models));
    }
}
