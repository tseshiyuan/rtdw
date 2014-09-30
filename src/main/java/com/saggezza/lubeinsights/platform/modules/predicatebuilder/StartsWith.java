package com.saggezza.lubeinsights.platform.modules.predicatebuilder;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import java.util.function.Predicate;
import java.util.function.Function;

/**
 * Created by chiyao on 9/25/14.
 */

/**
 * This function builds startWith predicate based by applying a matching string to it
 * For example,
 * Function<String,Predicate<DataElement>> function = (Function<String,Predicate<DataElement>>) Class.forName("com.saggezza.lubeinsights.platform.modules.predicatebuilder.StartsWith").newInstance();
 * Predicate<DataElement> filter = function.apply("###");
 * filter.apply(new DataElement(DataType.TEXT,"###comment"))  returns true
 */
public class StartsWith implements Function<String,Predicate<DataElement>> {

    public Predicate<DataElement> apply(String startsWith) {
        return new Predicate<DataElement>() {
            public boolean test(DataElement line) {
                return line.asText().startsWith(startsWith);
            }
        };
    }

}
