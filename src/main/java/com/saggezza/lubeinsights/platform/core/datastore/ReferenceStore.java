package com.saggezza.lubeinsights.platform.core.datastore;

/**
 * Created by chiyao on 9/18/14.
 */

import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;

/**
 * It is a persistent table in hbase or any plugin storage engine with latest content cached
 * Data updates happen indefinitely
 * It is a lookup data store used to enrich temporal store data
 */
public class ReferenceStore extends DataStore {

    public ReferenceStore(String name, DataModel dataModel, StorageEngine storageEngine) {
        super(name, dataModel, storageEngine);
    }

    public final void close() {
        // TODO
    }

}
