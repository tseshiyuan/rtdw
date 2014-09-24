package com.saggezza.lubeinsights.platform.core.dataengine.module;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;

import java.io.Serializable;

/**
 * @author : Albin
 *
 * Transformation on a data element.
 */
public interface Function extends java.util.function.Function<DataElement, DataElement>, Serializable {

    @Override
    DataElement apply(DataElement dataElement);

    DataModel apply(DataModel in) throws DataEngineExecutionException;
}

