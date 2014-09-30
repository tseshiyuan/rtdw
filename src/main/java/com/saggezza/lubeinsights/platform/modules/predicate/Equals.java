package com.saggezza.lubeinsights.platform.modules.predicate;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Predicate;

/**
 * @author : Albin
 */
public class Equals implements Predicate {

    private final String toCompare;

    public Equals(String toCompare) {
        this.toCompare = toCompare;
    }

    private Equals(Params in){
        this.toCompare = in.getFirst();
    }

    @Override
    public boolean test(DataElement dataElement) {
        return dataElement.asText().equals(toCompare);
    }
}
