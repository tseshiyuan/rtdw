package com.saggezza.lubeinsights.platform.core.datastore;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.FieldAddress;
import com.saggezza.lubeinsights.platform.core.serviceutil.ResourceManager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by chiyao on 9/18/14.
 */
public class StorageEngine {

    private String type;
    private String address;
    private StorageEngineClient client = null;

    public StorageEngine(String type,String address) {
        this.type = type;
        this.address=address;
    }

    /**
     *
     * @param temporalStoreName
     * @param windowName
     * @param groupByKeyAddress
     * @param aggFieldAddress
     * @return a corresponding storage engine client based on type, or null if unknown type
     */
    public StorageEngineClient getStorageEngineClient(String temporalStoreName, String windowName, FieldAddress[] groupByKeyAddress, String[] aggFieldAddress) {

        StorageEngineClient client = null;
        if (type.equalsIgnoreCase("hbase")) {
            client = getHBaseClient(temporalStoreName, windowName, groupByKeyAddress, aggFieldAddress);
        }
        else if (type.equalsIgnoreCase("file")) {
            client = getFileClient(temporalStoreName, windowName, groupByKeyAddress, aggFieldAddress);
        }
        else if (type.equalsIgnoreCase("sql")) {
            client = getSqlClient(temporalStoreName, windowName, groupByKeyAddress, aggFieldAddress);
        }
        this.client  = client;
        return client;
    }

    /**
     * generate a client and initialize the server
     * @param temporalStoreName         table name
     * @param windowName                rowkey (timestamp)
     * @param groupByKeyAddress         rowkey
     * @param aggFieldAddress           column family (stats names are columns/qualifiers)
     * @return a HBase StorageEngineClient
     */
    private StorageEngineClient getHBaseClient(String temporalStoreName, String windowName, FieldAddress[] groupByKeyAddress, String[] aggFieldAddress) {
        // This leads to HBase table = temporalStoreName, rowKey = <windowName,groupByKey values>,  column = <count,min,max,sum,sqsum>
        return null;
    }

    /**
     *
     * @param temporalStoreName        file name
     * @param windowName               1st field
     * @param groupByKeyAddress        following fields
     * @param aggFieldAddress          following fields
     * @return a file StorageEngineClient
     */
    private StorageEngineClient getFileClient(String temporalStoreName, String windowName, FieldAddress[] groupByKeyAddress, String[] aggFieldAddress) {

        File temporalStoreFile = new File(ResourceManager.allocateFile(temporalStoreName));
        // File temporalStoreFile = new File(address,temporalStoreName);

        StorageEngineClient client = new StorageEngineClient() {

            protected BufferedWriter output = null;

            public final boolean isOpen() {
                return output != null;
            }

            public void open() throws IOException {
                System.out.println("open client");
                if (output==null) {
                    output = new BufferedWriter(new FileWriter(temporalStoreFile));
                }
            }

            public void close() throws IOException {
                System.out.println("Close client");
                if (output != null) {
                    output.close();
                    output = null;
                }
            }

            /**
             * just plain insert the record for now
             * @param element
             * @throws IOException
             */
            public void aggsert(DataElement element) throws IOException {
                System.out.println("aggsert "+element.toString());
                output.write(element.toString());
                output.newLine();
            }
        };

        return client;

    }

    /**
     *
     * @param temporalStoreName        table name
     * @param windowName               key part1
     * @param groupByKeyAddress        key part2
     * @param aggFieldAddress          following fields
     * @return a sql StorageEngineClient
     */
    private StorageEngineClient getSqlClient(String temporalStoreName, String windowName, FieldAddress[] groupByKeyAddress, String[] aggFieldAddress) {
        return null;
    }


}
