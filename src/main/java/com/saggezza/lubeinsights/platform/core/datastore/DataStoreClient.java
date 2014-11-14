package com.saggezza.lubeinsights.platform.core.datastore;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRefType;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.serviceutil.*;
import org.apache.log4j.Logger;

import java.io.*;
import java.sql.*;
import java.util.*;

/**
 * Created by chiyao on 10/3/14.
 */

public class DataStoreClient implements AutoCloseable {

    public static final String UPSERT_BATCH_MODE = "w";
    public static final String QUERY_MODE = "r";
    public static final Logger logger = Logger.getLogger(DataStoreClient.class);
    public static final ServiceConfig config = ServiceConfig.load();
    protected static final String tenant = config.get("tenant");
    protected static final String application = config.get("application");
    protected String dataStoreName;
    protected DataModel dataModel;
    protected boolean isDerivedStore;
    protected String tableName;
    protected String[] fields;
    protected String[] indexFields;
    protected String[] regularFields;
    protected Connection conn = null;
    protected PreparedStatement wStmt = null; // for upsert only
    protected PreparedStatement lStmt = null; // for upsert only
    protected String mode;
    protected String batchId;
    protected transient StorePublisher storePublisher = null;

    public final boolean isOpen() {
        return conn != null;
    }

    public DataStoreClient(String dataStoreName) {
        this.dataStoreName = dataStoreName;
        tableName = StorageUtil.getTableName(tenant,application,dataStoreName);
        DataStore dataStore = DataStoreCatalog.getDataStore(tenant,application,dataStoreName);
        isDerivedStore = dataStore instanceof DerivedStore;
        dataModel = dataStore.getDataModel();
        fields = dataStore.getFields();
        indexFields = dataStore.getIndexFields();
        regularFields = dataStore.getRegularFields();
    }

    public String getDataStoreName() {
        return dataStoreName;
    }

    /**
     * open with UPSERT_BATCH_MODE or QUERY_MODE
     * @param mode
     * @throws IOException
     */
    public void open(String mode) throws IOException {
        Statement stmt = null;
        try {
            this.mode = mode;
            // tell data store manager to set up storage engine (make sure the table is there)
            // sendCommand(ServiceCommand.OPEN_STORE_R, Params.of(dataStoreName));
            if (conn == null) {
                String address = config.get("storage-engine").split(",")[1].trim();  // in config file:  storage-engine = hbase,localhost:2182
                //Properties prop = new Properties();
                //prop.setProperty("UpsertBatchSize","1");  // TODO
                conn = DriverManager.getConnection("jdbc:phoenix:" + address);
            }
            if (mode.equalsIgnoreCase(UPSERT_BATCH_MODE)) {
                // set up data table
                String sql = StorageUtil.genSqlCreateTable(tableName, indexFields, regularFields, dataModel);
                System.out.println(sql);
                stmt = conn.createStatement();
                stmt.executeUpdate(sql);
                // set up log table
                sql = StorageUtil.genSqlCreateLogTable(tableName, indexFields, regularFields, dataModel);
                System.out.println(sql);
                stmt = conn.createStatement();
                stmt.executeUpdate(sql);
                if (DataStoreCatalog.hasDerivedStore(tenant, application, dataStoreName)) {  // publish data for subscribing DerivedStore
                    storePublisher = new StorePublisher(dataStoreName, dataModel);
                    storePublisher.open();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IOException(e);
        }
    }

    public void close() throws IOException {
        try {
            logger.info("Close DataStoreClient "+ dataStoreName);
            if (wStmt != null) {
                wStmt.close();
                wStmt = null;
            }
            if (lStmt != null) {
                lStmt.close();
                lStmt = null;
            }
            if (conn != null) {
                conn.close();
                conn = null;
            }
            if (storePublisher != null) {
                storePublisher.close();
                storePublisher = null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IOException(e);
        }
    }


    public final void beginBatch(String tag) throws IOException {
        if (!UPSERT_BATCH_MODE.equalsIgnoreCase(this.mode)) {
            throw new IOException("beginBatch must be applied on data store open in mode \"w\"");
        }
        batchId = composeBatchId(tag);
    }

    private String composeBatchId(String tag) {
        return String.valueOf(System.currentTimeMillis()/1000)+tag;
    }

    public final void endBatch() {
        if (storePublisher != null) {
            storePublisher.endBatch(); // send an EOB token
        }
    }

    /**
     * assuming called after open()
     * @param element
     */
    public void upsert(DataElement element) throws IOException {
        try {
            // support only batch upsert for now
            if (!UPSERT_BATCH_MODE.equalsIgnoreCase(this.mode)) {
                throw new IOException("upsert must be applied on data store open in mode "+UPSERT_BATCH_MODE);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("upsert " + element.toString());
            }
            // assuming it's flat record for now. TODO: extend to structural elements
            if (wStmt == null) {
                String sql = StorageUtil.genSqlUpsert(tableName, fields);
                logger.info(sql);
                wStmt = conn.prepareStatement(sql);
            }
            // set values for all fields
            for (int i=0; i<fields.length; i++) {
                DataElement value = element.valueByName(fields[i]);
                if (value.isNumber()) {
                    wStmt.setDouble(i+1, value.asNumber().doubleValue());
                }
                else {
                    wStmt.setString(i+1, value.asText());
                }
            }
            wStmt.execute();
            // write to track table
            if (lStmt == null) {
                String sql = StorageUtil.genSqlUpsertLog(tableName, fields);
                logger.info(sql);
                lStmt = conn.prepareStatement(sql);
                lStmt = conn.prepareStatement(sql);
            }
            // set values for all fields
            for (int i=0; i<fields.length; i++) {
                DataElement value = element.valueByName(fields[i]);
                if (value.isNumber()) {
                    lStmt.setDouble(i+1, value.asNumber().doubleValue());
                }
                else {
                    lStmt.setString(i+1, value.asText());
                }
            }
            // set value for batchId
            lStmt.setString(fields.length+1,batchId);
            lStmt.execute();
            conn.commit(); // TODO: use auto commit

            // publish to kafka for derived stores if any
            if (storePublisher != null) {
                storePublisher.upsert(batchId,element);
            }

        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IOException(e);
        }
    }

    /**
     *
     * @param dataRefType
     * @param sql
     * @param fieldTypes  map a field name to true iff it's of type TEXT (false iff NUMBER)
     * @return
     * @throws IOException
     */
    private DataRef queryTable(DataRefType dataRefType, String sql, HashMap<String,Boolean> fieldTypes) throws IOException {
        PreparedStatement pstmt=null;
        try {
            System.out.println(sql);
            pstmt = conn.prepareStatement(sql);
            ResultSet rs = pstmt.executeQuery();

            ArrayList<DataElement> al = new ArrayList<DataElement>();
            switch (dataRefType) {
                case VALUE:
                    while (rs.next()) {
                        LinkedHashMap<String, DataElement> tm = new LinkedHashMap<String, DataElement>();
                        for (Map.Entry<String, Boolean> e : fieldTypes.entrySet()) {
                            if (e.getValue()) { // type is String
                                tm.put(e.getKey(), new DataElement(DataType.TEXT, rs.getString(e.getKey())));
                            } else {   // type is a number
                                tm.put(e.getKey(), new DataElement(DataType.NUMBER, rs.getDouble(e.getKey())));
                            }
                        }
                        al.add(new DataElement(tm));
                    }
                    return new DataRef(DataRefType.VALUE, al);
                case FILE:
                    String outFileName = ResourceManager.allocateFile("queryresult."+dataStoreName+"."+tenant+"."+application+"."+System.currentTimeMillis());
                    PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outFileName)));
                    while (rs.next()) {
                        LinkedHashMap<String, DataElement> tm = new LinkedHashMap<String, DataElement>();
                        for (Map.Entry<String, Boolean> e : fieldTypes.entrySet()) {
                            if (e.getValue()) { // type is String
                                tm.put(e.getKey(), new DataElement(DataType.TEXT, rs.getString(e.getKey())));
                            } else {   // type is a number
                                tm.put(e.getKey(), new DataElement(DataType.NUMBER, rs.getDouble(e.getKey())));
                            }
                        }
                        out.println(new DataElement(tm).toString());
                    }
                    out.flush();
                    out.close();
                    return new DataRef(DataRefType.VALUE, outFileName);
                default:
                    logger.error("Query result must be DataRef type VALUE or FILE");
                    return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
            throw new IOException(e);
        } finally {
            if (pstmt != null) {
                try {pstmt.close();} catch (Exception e){}
            }
        }
    }


    /**
     * put the result into the result DataRef
     * @param dataRefType     what type of data ref should we return (FILE or VALUE)
     * @param groupByFields
     * @param aggFields
     * @param whereClause
     * @throws IOException
     */
    private DataRef query(DataRefType dataRefType, String[] groupByFields, String[] aggFields,
                         String whereClause, String havingClause) throws IOException {
        if (!QUERY_MODE.equalsIgnoreCase(this.mode)) {
            throw new IOException("query must be applied on data store open in mode \"r\"");
        }
        if (isDerivedStore) {
            if (aggFields != null) {
                // translate max(age) to max(age.MAX), and avg(age) to sum(age.SUM)/sum(age.COUNT)
                String[] newAggFields = new String[aggFields.length];
                for (int i = 0; i < aggFields.length; i++) {
                    String[] expAlias = aggFields[i].split(" ");
                    newAggFields[i] = StorageUtil.transformAggExpression(expAlias[0]) + " " + expAlias[1];
                }
                aggFields = newAggFields;
            }
            // also do the same for having clause
            if (havingClause != null) {
                String[] having = havingClause.split(" ");
                StringBuilder sb = new StringBuilder();
                for (String s : having) {
                    sb.append(StorageUtil.transformAggExpression(s)).append(' ');
                }
                sb.setLength(sb.length() - 1);
                havingClause = sb.toString();
            }
        }
        String sql = StorageUtil.genSqlQuery(tableName, groupByFields, aggFields, whereClause, havingClause);
        logger.info(sql);
        HashMap<String,Boolean> fieldTypes = StorageUtil.genFieldTypes(dataModel,groupByFields);
        // add all aggFields types to it
        for (String aggField:aggFields) {
            fieldTypes.put(aggField.split(" ")[1],Boolean.FALSE);
        }
        return queryTable(dataRefType, sql, fieldTypes);
    }

    public DataRef query(DataRefType dataRefType, String[] fields, String whereClause) throws IOException {
        if (!QUERY_MODE.equalsIgnoreCase(this.mode)) {
            throw new IOException("query must be applied on data store open in mode \"r\"");
        }
        if (isDerivedStore) {
            String[] groupByFields = intersect(indexFields,fields);
            String[] aggFields = intersect(regularFields,fields);
            // transform age.MAX to "max(age.MAX) agemax"
            String[] newAggFields = new String[aggFields.length];
            for (int i = 0; i < aggFields.length; i++) {
                String[] fieldAggregator = aggFields[i].split("\\."); // "age" and "MAX"
                newAggFields[i] = fieldAggregator[1]+"("+aggFields[i]+") "+fieldAggregator[0]+fieldAggregator[1];
            }
            aggFields = newAggFields;
            return query(dataRefType, groupByFields, aggFields, whereClause, null);
        }
        else {
            String sql = StorageUtil.genSqlQuery(tableName, fields, whereClause);
            logger.info(sql);
            HashMap<String, Boolean> fieldTypes = StorageUtil.genFieldTypes(dataModel, fields);
            return queryTable(dataRefType, sql, fieldTypes);
        }
    }

    private String[] intersect(String[] s1, String[] s2) {
        LinkedHashSet<String> hs1 = new LinkedHashSet<String>();
        for (String s: s1) {
            hs1.add(s);
        }
        LinkedHashSet<String> hs2 = new LinkedHashSet<String>();
        for (String s: s2) {
            hs2.add(s);
        }
        hs1.retainAll(hs2);
        return (String[])hs1.toArray(new String[0]);
    }


    public static final void main(String[] args) {
        DataStoreClient client = null;
        try {
            // write
            /*
            DataStore store = DataStoreCatalog.getDataStore(config.get("tenant"), config.get("application"), "store01");
            client = new DataStoreClient("store01");
            System.out.println(store.serialize());
            System.out.println(store.getDataModel().toJson());
            client.open(UPSERT_BATCH_MODE);
            ArrayList<DataElement> al = StorageUtil.readFromFile(args[0],store.getDataModel());
            for (DataElement e: al) {
                client.upsert(e);
                System.out.println(e);
            }
            */
            /*
            // read
            DataStore store = DataStoreCatalog.getDataStore(config.get("tenant"), config.get("application"), "store02");
            client = new DataStoreClient("store02");
            System.out.println(store.serialize());
            System.out.println(store.getDataModel().toJson());
            client.open(QUERY_MODE);
            DataRef dataRef = client.query(DataRefType.VALUE,new String[] {"createDate","gender","age.COUNT"},null);
            ArrayList<DataElement> result = (ArrayList<DataElement>) dataRef.getValue();
            for (DataElement e: result) {
                System.out.println(e);
            }
            */

            // query
            client = new DataStoreClient("store02");
            client.open(QUERY_MODE);

            DataRef dataRef = client.query(DataRefType.VALUE,
                    new String[] {"createDate","gender"},
                    new String[] {"max(age) max_age"},
                    "createDate > 100", // where
                    "max(age) > 30");  // having

            /*
            DataRef dataRef = client.query(DataRefType.VALUE,
                    new String[] {"createDate","gender","age.MAX"},
                    null
            );
            */
            ArrayList<DataElement> result = (ArrayList<DataElement>) dataRef.getValue();
            for (DataElement e: result) {
                System.out.println(e);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (client != null) {
                try {
                    client.close();
                } catch (Exception e) {}
            }
        }
    }
}
