package com.saggezza.lubeinsights.platform.modules.aggregator;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Aggregator;

/**
 * @author : Albin
 *
 * Finds the min between the two numbers.
 */
public class Min implements Aggregator {

    public Min(Params params) {
    }

    @Override
    public Number aggregate(Number one, Number two) {
        return one.doubleValue() < two.doubleValue() ? one : two;
    }
}
