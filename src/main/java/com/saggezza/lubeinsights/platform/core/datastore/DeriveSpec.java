package com.saggezza.lubeinsights.platform.core.datastore;

import com.google.common.base.Joiner;
import com.saggezza.lubeinsights.platform.core.common.GsonUtil;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.FieldAddress;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.common.modules.ModuleFactory;

import java.util.Arrays;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by chiyao on 9/18/14.
 */
public class DeriveSpec {

    private String spec; // string representation of this DeriveSpec (for serialization purpose)
    private transient FieldAddress[] aggFieldAddress;
    private transient String[] aggFieldAlias;
    private transient FieldAddress[] groupByKeyAddress;
    private transient FieldAddress[] temporalKeyAddress;
    private transient Predicate<DataElement> filter;
    private transient Function<DataElement[],Long> windowFunction;
    private transient String windowName;
    private transient String[] groupByNames;
    // this transformer function is derived from the info above
    private transient Function<DataElement,DataElement> dataElementTransformer;
    private transient FieldAddress[] derivedGroupByKeyAddress;

    /**
     * spec is ";" delimited fields, representing filterName;addFieldAddress;groupByKeys;temporalKeys;windowFunctionName;windowName
     * @param spec
     */
    public DeriveSpec(String spec) {
        this.spec = spec;
        compile();
    }

    public DeriveSpec(String filterName,String aggFields,String aggFieldAlias,String groupByFields,
                      String temporalKeys,String windowFunction,String windowName) {
        this(new StringBuilder()
                .append(filterName == null ? "" : filterName).append(";")
                .append(aggFields == null ? "" : aggFields).append(";")
                .append(aggFieldAlias == null ? "" : aggFieldAlias).append(";")
                .append(groupByFields == null ? "" : groupByFields).append(";")
                .append(temporalKeys == null ? "" : temporalKeys).append(";")
                .append(windowFunction == null ? "" : windowFunction).append(";")
                .append(windowName == null ? "" : windowName).toString());
    }

    /**
     * For example, we can derive the TemporalStore transaction-activities into another TemporalStore with this spec:
     *
     * select date(activityTime) activityDate, city, productId, agg(transactionPrice) AggPrice
     * from transaction-activities
     * group by date(activityTime) activityDate, city, productId
     * where productId startsWith 001
     *
     * filterName          is     productIdStartsWith001
     * aggFieldAddress     is     [[transactionPrice]]
     * aggFieldAlias       is     [AggPrice]
     * groupByKeyAddress   is     [[city], [productId]]
     * groupByNames        is     [city,productId]
     * temporalKeys        is     [activityTime]
     * windowFunctionName  is     date
     * windowName          is     activityDate
     */
    public void compile() {

       String[] specFields = spec.split(";");
        // convert empty string to null
        for (int i=0; i<specFields.length; i++) {
            if (specFields[i].isEmpty()) {
                specFields[i]=null;
            }
        }
        if (specFields[0] != null) {
            filter = (Predicate<DataElement>) ModuleFactory.getModule("predicate", specFields[0]); // filter name
        }
       aggFieldAddress = FieldAddress.generateFieldAddresses(specFields[1]);  // agg fields address
       aggFieldAlias = StringToArray(specFields[2]);  // agg fields alias (non-null)
       groupByNames = StringToArray(specFields[3]);
       groupByKeyAddress = FieldAddress.generateFieldAddresses(specFields[3]); // group key address (non-null)
       temporalKeyAddress = FieldAddress.generateFieldAddresses(specFields[4]); // temporal key address (non-null)
       if (specFields[5] != null) {
            windowFunction = (Function<DataElement[], Long>) ModuleFactory.getModule("transform", specFields[5]); // window function name
       }
       else {  // just retrieve windowName without conversion
           windowFunction = (temporalFields)-> temporalFields[0].asNumber().longValue();
       }
       windowName = specFields[6]; // window name (non-null)

       // define how to transform a data element in source TemporalStore into one in target TemporalStore
       dataElementTransformer = (elt) -> transformElement(elt); // e.g. element of form: <dateOfActivityTime, city, productId, transactionPrice>
       // derived fields
       derivedGroupByKeyAddress = new FieldAddress[groupByKeyAddress.length]; // [[city],[productId]]
       // [[a,b],[c,d]] becomes [[a_b],[c_d]], and [[a],[b]] becomes [[a],[b]]
       for (int i=0; i<groupByKeyAddress.length; i++) {
           Object[] coordinate = groupByKeyAddress[i].getCoordinate();
           String[] coord = Arrays.copyOf(coordinate, coordinate.length, String[].class);  // cast
           derivedGroupByKeyAddress[i] = new FieldAddress(Joiner.on(".").join(coord));
       }
    }


    public final Function<DataElement,DataElement> getDataElementTransformer() {
        return dataElementTransformer;
    }

    /**
     * transform an element based on this derive spec
     * select from elt: temporalKeys, groupByKeys, aggFields in order
     * @param elt
     * @return
     */
    private DataElement transformElement(DataElement elt) {
        if (filter != null && !filter.test(elt)) {
            return null;
        }
        TreeMap<String,DataElement> map = new TreeMap<String,DataElement>();
        // add activityDate
        long windowId = windowFunction.apply(elt.getFields(temporalKeyAddress));
        map.put(windowName,new DataElement(DataType.NUMBER,windowId));
        // add city,productId
        DataElement[] groupByElts = elt.getFields(groupByKeyAddress);
        for (int i=0; i<groupByNames.length; i++) {
            map.put(groupByNames[i],groupByElts[i]);
        }
        // add transactionPrice
        DataElement[] aggElts = (aggFieldAddress==null ? null : elt.getFields(aggFieldAddress));
        if (aggElts == null) {  // if no field needs to aggregate, there must be only one alias for agg
            map.put(aggFieldAlias[0],DataElement.EMPTY); // EMPTY means no value needed for this agg field (because we track only counts)
        }
        else {
            for (int i = 0; i < aggFieldAlias.length; i++) {
                map.put(aggFieldAlias[i], aggElts[i]);
            }
        }
        return new DataElement(map); // return the transformed element
    }

    public final String getDerivedTemporalKeyName() {
        return windowName;
    }

    public final FieldAddress[] getDerivedGroupByKeyAddress() {
        return derivedGroupByKeyAddress;
    }

    public final String[] getAggFieldAlias() {return aggFieldAlias;}


    public final String toJson() {
        return GsonUtil.gson().toJson(this);
    }

    public static final DeriveSpec fromJson(String json) {
        DeriveSpec deriveSpec = GsonUtil.gson().fromJson(json,DeriveSpec.class);
        deriveSpec.compile();
        return deriveSpec;
    }


    /**
     * convert [s1,s2,s3] to String[] {"s1","s2","s3"}
     * @param s
     */
    private static final String[] StringToArray(String s) {
        if (s.startsWith("[") && s.endsWith("]")) {
            String[] array = s.substring(1,s.length()-1).split(",");
            for (int i=0; i<array.length; i++) {
                array[i] = array[i].trim();
            }
            return array;
        }
        else throw new RuntimeException("Bad string representation for an array: "+s);
    }

    /**
     * derive from dataModel to a new one based on this spec
     * @param dataModel
     * @return
     */
    public final DataModel deriveDataModel(DataModel dataModel) {
        TreeMap<String,DataModel> tm =  new TreeMap<String,DataModel>();
        // build model for aggField part
        for (int i=0; i<aggFieldAlias.length;i++) {
            tm.put(aggFieldAlias[i],DataModel.statsDataModel); // each agg field is a stats list (count,min,max,sum,sqsum)
        }
        // build model for groupBy part
        for (int i=0; i<groupByNames.length;i++) {
            tm.put(groupByNames[i],dataModel.getField(derivedGroupByKeyAddress[i]));
        }
        // build model for window part
        tm.put(windowName,new DataModel(DataType.NUMBER));
        // return the result data model
        return new DataModel(tm);
    }


}
