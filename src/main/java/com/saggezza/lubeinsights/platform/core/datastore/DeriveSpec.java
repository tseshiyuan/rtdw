package com.saggezza.lubeinsights.platform.core.datastore;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by chiyao on 9/18/14.
 */
public class DeriveSpec {

    private Object[][] aggFieldAddress;
    private Object[][] groupByKeyAddress;
    private Object[][] temporalKeyAddress;
    private Predicate<DataElement> filter;
    private Function<DataElement[],Long> windowFunction;
    private String windowName;
    private String[] groupByNames;
    private String[] aggNames;
    // this transformer function is derived from the info above
    private Function<DataElement,DataElement> dataElementTransformer;
    private Object[][] derivedGroupByKeyAddress;
    private Object[][] derivedAggFieldAddress;


    /**
     * For example, we can derive the TemporalStore transaction-activities into another TemporalStore with this spec:
     *
     * select date(activityTime) activityDate, city, productId, agg(transactionPrice)
     * from transaction-activities
     * group by date(activityTime), city, productId
     * where productId startsWith 001
     *
     * @param filter          is     productId startsWith 001
     * @param aggFieldAddress is     transactionPrice
     * @param groupByKeys     is     city, productId
     * @param temporalKeys    is     activityTime
     * @param windowFunction  is     date()
     * @param windowName      is     activityDate
     */
    public DeriveSpec(Predicate<DataElement> filter, String aggFieldAddress, String groupByKeys, String temporalKeys,
                      Function<DataElement[],Long> windowFunction, String windowName) {
        // TODO: handle null parameters (Can a derived store not aggregate?)
        this.aggFieldAddress = DataElement.generateCoordinates(aggFieldAddress);
        this.groupByKeyAddress = DataElement.generateCoordinates(groupByKeys);
        this.temporalKeyAddress = DataElement.generateCoordinates(temporalKeys);
        this.filter = filter;
        this.windowFunction = windowFunction;
        this.windowName = windowName;
        groupByNames = groupByKeys.split(",");
        aggNames = aggFieldAddress.split(",");
        // define how to transform a data element in source TemporalStore into one in target TemporalStore
        dataElementTransformer = (elt) -> transformElement(elt); // e.g. element of form: <dateOfActivityTime, city, productId, transactionPrice>
        // derived fields
        derivedGroupByKeyAddress = groupByKeyAddress; // same name as original, [[city],[productId]]
        derivedAggFieldAddress = this.aggFieldAddress; // same as original, [[transactionPrice]]
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
        if (!filter.test(elt)) {
            return null;
        }
        HashMap<String,DataElement> map = new HashMap<String,DataElement>();
        // add date_ActivityTime
        long windowId = windowFunction.apply(elt.getElements(temporalKeyAddress));
        map.put(windowName,new DataElement(DataType.NUMBER,windowId));
        // add city,productId
        DataElement[] groupByElts = elt.getElements(groupByKeyAddress);
        for (int i=0; i<groupByNames.length; i++) {
            map.put(groupByNames[i],groupByElts[i]);
        }
        // add transactionPrice
        DataElement[] aggElts = elt.getElements(aggFieldAddress);
        for (int i=0; i<aggNames.length; i++) {
            map.put(aggNames[i],aggElts[i]);
        }
        return new DataElement(map); // return the transformed element
    }

    public final String getDerivedTemporalKeyName() {
        return windowName;
    }

    public final Object[][] getDerivedGroupByKeyAddress() {
        return derivedGroupByKeyAddress;
    }

    public final Object[][] getDerivedAggFieldAddress() {
        return derivedAggFieldAddress;
    }

}
