package com.saggezza.lubeinsights.platform.modules.predicate;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Predicate;


/**
 * @author : Albin
 *
 * Checks whether a specific string exists in the data element.
 */
public class Contains implements Predicate<DataElement> {

    private final String contains;

    public Contains(String contains) {
        this.contains = contains;
    }

    private Contains(Params args){
        this(args.<String>get(0));
    }

    @Override
    public boolean test(DataElement line) {
        return line.asText().contains(contains);
    }
}
