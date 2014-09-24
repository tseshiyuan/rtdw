package com.saggezza.lubeinsights.platform.core.dataengine.module;

import java.io.Serializable;

/**
 * @author : Albin
 *
 * Associative function to reduce two numbers into one.
 */
public interface Aggregator extends Serializable {

    Number aggregate(Number one, Number two);//TODO - vaidate the typing here as number

}
