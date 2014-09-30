package com.saggezza.lubeinsights.platform.core.dataengine.module;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;

import java.io.Serializable;

/**
 * @author : Albin
 */
public interface Predicate extends java.util.function.Predicate<DataElement>, Serializable{
}
