package com.saggezza.lubeinsights.platform.modules.predicate;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.modules.Modules;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Predicate;

/**
 * @author : Albin
 */
public class ColumnPredicate implements Predicate {

    private String name;
    private int index = -1;
    private Predicate predicate;

    public ColumnPredicate(){}

    private ColumnPredicate(Params in){
        Object first = in.getFirst();
        if(first instanceof String){
            name = (String) first;
        }else if(first instanceof Integer){
            index = (Integer) first;
        }
        this.predicate = Modules.predicate(in.getSecond());
    }

    @Override
    public boolean test(DataElement dataElement) {
        if(index == -1){
            return predicate.test(dataElement.valueByName(name));
        }else {
            return predicate.test(dataElement.valueAt(index));
        }
    }

}
