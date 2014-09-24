package com.saggezza.lubeinsights.platform.core.datastore;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;

import java.io.IOException;

/**
 * Created by chiyao on 9/18/14.
 */
public interface StorageEngineClient {

    public boolean isOpen();
    public void open() throws IOException;
    public void close() throws IOException;
    public void aggsert(DataElement dataElement) throws IOException; //aggregate if exists, or insert if not exists
}
