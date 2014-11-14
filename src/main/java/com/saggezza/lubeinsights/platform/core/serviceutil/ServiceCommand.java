package com.saggezza.lubeinsights.platform.core.serviceutil;

/**
 * Created by chiyao on 7/16/14.
 */

/**
 * This class describes the spec of a platform serviceutil
 */
public enum ServiceCommand {

    // WORKFLOW_ENGINE
    RUN_WORKFLOW(ServiceName.WORKFLOW_ENGINE),

    // DATA_ENGINE
    DefineInput(ServiceName.DATA_ENGINE),
    Load(ServiceName.DATA_ENGINE),
    Publish(ServiceName.DATA_ENGINE),
    PARSE(ServiceName.DATA_ENGINE),
    Map(ServiceName.DATA_ENGINE),
    Filter(ServiceName.DATA_ENGINE),
    Browse(ServiceName.DATA_ENGINE),
    GroupBy(ServiceName.DATA_ENGINE),
    Join(ServiceName.DATA_ENGINE),
    READ(ServiceName.DATA_ENGINE),
    Dedup(ServiceName.DATA_ENGINE),
    Write(ServiceName.DATA_ENGINE),

    // COLLECTION_ENGINE
    COLLECT_BATCH(ServiceName.COLLECTION_ENGINE),
    START_COLLECTING_STREAM(ServiceName.COLLECTION_ENGINE),
    STOP_COLLECTING_STREAM(ServiceName.COLLECTION_ENGINE),

    // DATASTORE_MANAGER
    NEW_STORE(ServiceName.DATASTORE_MANAGER),
    NEW_DERIVED_STORE(ServiceName.DATASTORE_MANAGER),
    START_DERIVED_STORE(ServiceName.DATASTORE_MANAGER),
    STOP_DERIVED_STORE(ServiceName.DATASTORE_MANAGER),
    DELETE_STORE(ServiceName.DATASTORE_MANAGER),
    START_ALL_DERIVED_STORES(ServiceName.DATASTORE_MANAGER),
    STOP_ALL_DERIVED_STORES(ServiceName.DATASTORE_MANAGER),

    // STREAM_ENGINE
    AGGREGATE(ServiceName.STREAM_ENGINE),
    SPLIT(ServiceName.STREAM_ENGINE),
    SNAPSHOT(ServiceName.STREAM_ENGINE),

    // LOGGING_ENGINE
    CHECKPOINT(ServiceName.LOGGING_ENGINE),

    // RESOURCE_MGR
    GETFILE(ServiceName.RESOURCE_MGR),
    GETTABLE(ServiceName.RESOURCE_MGR),
    GETCHANNEL(ServiceName.RESOURCE_MGR),
    FREEFILE(ServiceName.RESOURCE_MGR),
    FREETABLE(ServiceName.RESOURCE_MGR),
    FREECHANNEL(ServiceName.RESOURCE_MGR)
    ;

    private ServiceName parent = null;
    private ServiceCommand(ServiceName parent) {
       this.parent = parent;
    }
    public ServiceName getParent() { return parent;}


}
