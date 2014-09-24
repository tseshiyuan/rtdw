package com.saggezza.lubeinsights.platform.modules.aggregator;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Aggregator;

/**
 * @author : Albin
 *
 * Finds the sum between the two numbers.
 */
public class Sum implements Aggregator {

    public Sum(Params params) {
    }

    @Override
    public Number aggregate(Number one, Number two) {
        return one.doubleValue() + two.doubleValue();
    }
}
