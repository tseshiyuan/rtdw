package com.saggezza.lubeinsights.platform.modules.datamodel.validator;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;

/**
 * Created by chiyao on 8/21/14.
 */
public class IsShortString implements ValidatorModule {

    public boolean validate(DataElement e) {
        if (e.getTypeIfPrimitive() != DataType.TEXT) {
            return false;
        }
       return (e.asText().length()<5);
    }
}
