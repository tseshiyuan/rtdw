package com.saggezza.lubeinsights.platform.modules.aggregator;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Aggregator;

/**
 * @author : Albin
 *
 * Averages the two numbers
 */
public class Average implements Aggregator{

    public Average(Params params) {
    }

    @Override
    public Number aggregate(Number one, Number two) {
        return (one.doubleValue() + two.doubleValue())/2;
    }
}
