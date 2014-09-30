package com.saggezza.lubeinsights.platform.modules.predicate;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Predicate;


/**
 * @author : Albin
 *
 * Checks if the input ends with a specific string.
 */
public class EndsWith implements Predicate {

    private final String endsWith;

    public EndsWith(String endsWith) {
        this.endsWith = endsWith;
    }

    private EndsWith(Params args){
        this(args.<String>get(0));
    }

    @Override
    public boolean test(DataElement line) {
        return line.asText().endsWith(endsWith);
    }
}
