package com.saggezza.lubeinsights.platform.modules.predicate;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Predicate;


/**
 * @author : Albin
 *
 * Checks whether the input starts with a specific string.
 */
public class StartsWith implements Predicate<DataElement> {

    private final String startsWith;

    public StartsWith(String startsWith) {
        this.startsWith = startsWith;
    }

    private StartsWith(Params args){
        this((String)args.get(0));
    }

    @Override
    public boolean test(DataElement line) {
        return line.asText().startsWith(startsWith);
    }
}
