package com.saggezza.lubeinsights.platform.modules.predicate;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Predicate;
import com.saggezza.lubeinsights.platform.core.common.modules.Modules;


/**
 * @author : Albin
 *
 * Ands the two predicates.
 */
public class And implements Predicate {

    private final Predicate one;
    private final Predicate two;

    public And(Predicate one, Predicate two) {
        this.one = one;
        this.two = two;
    }

    private And(Params args){
        one = Modules.predicate(args.get(0));
        two = Modules.predicate(args.get(1));
    }

    @Override
    public boolean test(DataElement o) {
        return one.test(o) && two.test(o);
    }
}
