package com.saggezza.lubeinsights.platform.core.datastore;

import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;

/**
 * Created by chiyao on 9/15/14.
 */
public class Schema {

    protected DataModel dataModel;
    protected StorageParadigm storageParadigm; // relation, columnar, key-based cache, graph db, stream, files
    protected AccessKey[] accessKeys; // guide the storage access with access keys, including combined keys
}