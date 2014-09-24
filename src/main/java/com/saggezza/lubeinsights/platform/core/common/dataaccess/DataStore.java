package com.saggezza.lubeinsights.platform.core.common.dataaccess;

/**
 * Created by chiyao on 9/5/14.
 */

import java.util.concurrent.Future;

/**
 *
 * Data Store is a data container that supports insert, query, search operations
 * Unlike DataRef, its content is not frozen, and it can be backed by various storage engines.
 * Data Store can be used as a sink of data collection or data pipe, or as a the source for analytical queries
 */
public abstract class DataStore {

    Schema schema;  // describes data model, storage paradigm (relational, columnar, key based memory

    // define how to insert a data element
    public abstract void insert(DataElement element);

    // define the query processor
    public abstract Future<DataRef> query(String query); // query language TBD

}
