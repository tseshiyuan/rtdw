package com.saggezza.lubeinsights.platform.modules.predicate;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Predicate;
import com.saggezza.lubeinsights.platform.core.common.modules.Modules;


/**
 * @author : Albin
 *
 * Negates the incoming predicate
 */
public class Not implements Predicate {
    private final Predicate negate;

    public Not(Predicate negate) {
        this.negate = negate;
    }

    private Not(Params args){
        negate = Modules.predicate(args.get(0));
    }

    @Override
    public boolean test(DataElement o) {
        return !this.negate.test(o);
    }
}
