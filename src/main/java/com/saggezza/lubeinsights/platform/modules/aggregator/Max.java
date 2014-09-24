package com.saggezza.lubeinsights.platform.modules.aggregator;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Aggregator;

/**
 * @author : Albin
 *
 * Finds max between two numbers.
 */
public class Max implements Aggregator {

    public Max(Params params) {
    }

    @Override
    public Number aggregate(Number one, Number two) {//TODO - validate the double conversion as supporting all
        return one.doubleValue() > two.doubleValue() ? one : two;
    }
}
