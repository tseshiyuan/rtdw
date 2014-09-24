package com.saggezza.lubeinsights.platform.modules.datamodel.validator;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;

/**
 * Created by chiyao on 8/21/14.
 */
public interface ValidatorModule {
    public boolean validate(DataElement e);
}
