package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author : Albin
 * Converts the data into date as specified by the input pattern.
 */
public class ConvertToDate implements Function {

    private DateFormat pattern;

    public ConvertToDate(){
    }

    private ConvertToDate(Params params){
        String patternText = null == params.getFirst() ? "MM-dd-yyyy" : params.getFirst();
        this.pattern = new SimpleDateFormat(patternText);
    }

    @Override
    public DataElement apply(DataElement in) {
        try {
            Date parsed = this.pattern.parse(in.asText());
            return new DataElement(DataType.DATETIME, parsed);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DataModel apply(DataModel in) {
        return new DataModel(DataType.DATETIME);
    }
}
