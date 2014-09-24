package com.saggezza.lubeinsights.platform.modules.dataset;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.Environment;

/**
 * Created by chiyao on 7/30/14.
 */
public interface DataSetModule {
    // dataaccess set module transforms dataaccess referenced by inputDataRef into dataaccess referenced by outputDataRef
    public void run(Environment environment, DataRef inputDataSet,  DataRef outputDataSet);
}
