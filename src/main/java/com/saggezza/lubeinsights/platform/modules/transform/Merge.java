package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.Selection;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;
import org.apache.commons.lang.StringUtils;

/**
 * Created by venkateswararao on 13/10/14.
 */
public class Merge implements Function {

    private Selection selection;
    private String delimiter;

    Merge() {
    }

    public Merge(Params params) {
        selection = params.getFirst();
        delimiter = params.size() > 1 ? params.getSecond() : StringUtils.EMPTY;
    }

    @Override
    public DataElement apply(DataElement record) {
        StringBuilder resultValue = new StringBuilder();
        for (DataElement element : record.select(selection).allValues()) {
            resultValue.append(element.value()).append(delimiter);
        }
        record.asList().add(new DataElement(DataType.TEXT, StringUtils.isEmpty(delimiter) ? resultValue.toString() :
                resultValue.toString().substring(0, resultValue.length() - 1)));
        return new DataElement(record.allValues());
    }

    @Override
    public DataModel apply(DataModel in) throws DataEngineExecutionException {
        return in;
    }
}
