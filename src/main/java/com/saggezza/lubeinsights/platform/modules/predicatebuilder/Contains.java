package com.saggezza.lubeinsights.platform.modules.predicatebuilder;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Predicate;

import java.util.function.Function;

/**
 * Created by chiyao on 9/25/14.
 */

/**
 * This function builds startWith predicate based by applying a matching string to it
 * For example,
 * Function<String,Predicate<DataElement>> function = (Function<String,Predicate<DataElement>>) Class.forName("com.saggezza.lubeinsights.platform.modules.predicatebuilder.Contains").newInstance();
 * Predicate<DataElement> filter = function.apply("###");
 * filter.apply(new DataElement(DataType.TEXT,"###comment"))  returns true
 */
public class Contains implements Function<String,Predicate<DataElement>> {

    public Predicate<DataElement> apply(String contains) {
        return new Predicate<DataElement>() {
            public boolean test(DataElement line) {
                return line.asText().contains(contains);
            }
        };
    }

}
