package com.saggezza.lubeinsights.platform.core.datastore;

/**
 * Created by chiyao on 9/18/14.
 */

import com.saggezza.lubeinsights.platform.core.serviceutil.PlatformService;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceName;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceRequest;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceResponse;

/**
 * This class manages all the data stores
 */
public class DataStoreManager extends PlatformService {

    public DataStoreManager() {super(ServiceName.DATASTORE_MANAGER);}

    public ServiceResponse processRequest(ServiceRequest request) {
        return null;  // TODO
    }
}
