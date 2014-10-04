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

    private String type;
    private String address;

    public StorageEngine(String type,String address) {
        this.type = type;
        this.address=address;
    }


    /**
     * called by DataStore constructor
     * create table if not exists
     * @param dataStore
     */
    public final void setupDataStore(DataStore dataStore) {
        if (type.equalsIgnoreCase("file")) {
            return; // do nothing
        }
        // set up hbase (create table if not exists)
        Connection conn = null;
        CallableStatement cstmt = null;
        try {
            conn = DriverManager.getConnection("jdbc:phoenix:" + address);
            String sql = genSqlCreateTable(dataStore.getName(),dataStore.getIndexFields(),dataStore.getRegularFields(),dataStore.getDataModel());
            cstmt = conn.prepareCall(sql);
            cstmt.execute();
        } catch (SQLException e) {
            logger.trace(e);
            e.printStackTrace();
        } finally {
            if (cstmt != null) {
                try {
                    cstmt.close();
                } catch (Exception e) {
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception e) {
                }
            }
        }
    }

    /**
     * generate an upsert statement
     * @param table
     * @param fields
     * @return
     */
    private static final String genSqlUpsert(String table, String[] fields) {
        StringBuilder sb = new StringBuilder("upsert into ")
                .append(table).append(" (").append(StringUtils.join(fields,","))
                .append(") values (");
        for (int i=0; i<fields.length;i++) {
            sb.append("?,");
        }
        sb.setLength(sb.length()-1);
        sb.append(")");
        return sb.toString();
    }

    /**
     * e.g. create table if not exists us_population (col1 varchar, col2 varchar, col3 varchar constraint pk primary key (col1,col2));
     * @param table
     * @param indexFields
     * @param regularFields
     * @param dataModel
     * @return
     */
    private static final String genSqlCreateTable(String table, String[] indexFields, String[] regularFields, DataModel dataModel) {

        /**
         * example:
         * create table if not exists us_population
         * (col1 varchar, col2 varchar, col3 varchar constraint pk primary key (col1,col2));
         */
        StringBuilder sb = new StringBuilder("create table if not exists ").append(table).append(" (");
        for (String f: indexFields) {
            sb.append(f).append(" ").append(modelType2sqlType(dataModel.typeAt(f))).append(",");
        }
        if (regularFields != null) {
            for (String f: regularFields) {
                sb.append(f).append(" ").append(modelType2sqlType(dataModel.typeAt(f))).append(",");
            }
        }
        sb.setLength(sb.length()-1);
        sb.append(" constraint pk primary key (").append(StringUtils.join(indexFields,",")).append(")");
        return sb.toString();
    }

    /**
     * convert DataType to phoenix data type
     * @param dataType
     * @return
     */
    private static final String modelType2sqlType(DataType dataType) {
        switch (dataType) {
            case NUMBER: return "FLOAT";
            case TEXT: return "VARCHAR";
            case DATETIME: return "DATE";
            default: return null; // null maps to null
        }
    }

    /**
     *
     * @param temporalStoreName
     * @param keyNames
     * @param aggFieldNames
     * @return a corresponding storage engine client based on type, or null if unknown type
     */
    public StorageEngineClient getStorageEngineClient(String temporalStoreName, String[] keyNames, String[] regularFieldNames, String[] aggFieldNames) {

        StorageEngineClient client = null;
        String[] fields = ObjectArrays.concat(ObjectArrays.concat(keyNames, regularFieldNames, String.class),aggFieldNames,String.class);
        if (type.equalsIgnoreCase("hbase")) {
            client = getHBaseClient(temporalStoreName,fields);
        }
        else if (type.equalsIgnoreCase("file")) {
            client = getFileClient(temporalStoreName,fields);
        }
        return client;
    }

    /**
     * generate a client and initialize the server
     * @param fields                field names
     * @return a HBase StorageEngineClient
     */
    private StorageEngineClient getHBaseClient(String tableName, String[] fields) {
        // This leads to HBase table = temporalStoreName, rowKey = <windowName,groupByKey values>,  column = <count,min,max,sum,sqsum>
        StorageEngineClient client = new StorageEngineClient() {

            protected Connection conn = null;
            PreparedStatement pstmt = null;

            public final boolean isOpen() {
                return conn != null;
            }

            public void open() throws IOException {
                try {
                    System.out.println("open client");
                    if (conn == null) {
                        conn = DriverManager.getConnection("jdbc:phoenix:" + address);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error(e.getMessage());
                    throw new IOException(e);
                }
            }

            public void close() throws IOException {
                try {
                    System.out.println("Close client");
                    if (pstmt != null) {
                        pstmt.close();
                        pstmt = null;
                    }
                    if (conn != null) {
                        conn.close();
                        conn = null;
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error(e.getMessage());
                    throw new IOException(e);
                }
            }

            /**
             * just plain insert the record for now
             * @param element
             * @throws IOException
             */
            public void aggsert(DataElement element) throws IOException {
                try {
                    System.out.println("aggsert " + element.toString());
                    // assuming it's flat record for now. TODO: extend to structural elements
                    if (pstmt == null) {
                        String sql = genSqlUpsert(tableName, fields);
                        pstmt = conn.prepareStatement(sql);
                    }
                    // set values for all fields
                    for (int i=0; i<fields.length; i++) {
                        DataElement value = element.valueByName(fields[i]);
                        if (value.isNumber()) {
                            pstmt.setFloat(i, ((Float) value.asNumber()).floatValue());
                        }
                        else {
                            pstmt.setString(i, value.asText());
                        }
                    }
                    pstmt.execute();

                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error(e.getMessage());
                    throw new IOException(e);
                }
            }
        };

        return client;
    }

    /**
     *
     * @param temporalStoreName        file name
     * @param fields                   field names
     * * @return a file StorageEngineClient
     */
    private StorageEngineClient getFileClient(String temporalStoreName, String[] fields) {

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

}
