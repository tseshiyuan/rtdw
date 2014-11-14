package com.saggezza.lubeinsights.platform.core.datastore;

import com.google.common.collect.ObjectArrays;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.FieldAddress;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.serviceutil.ResourceManager;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;

/**
 * Created by chiyao on 9/18/14.
 */
public class StorageEngine {

    public static final Logger logger = Logger.getLogger(DataStoreManager.class);
    public static final ServiceConfig config = ServiceConfig.load();

    private String type;
    private String address;

    public StorageEngine(String type,String address) {
        this.type = type;
        this.address=address;
    }

}
