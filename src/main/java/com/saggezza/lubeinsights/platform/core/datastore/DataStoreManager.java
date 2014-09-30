package com.saggezza.lubeinsights.platform.core.datastore;

/**
 * Created by chiyao on 9/18/14.
 */

import com.saggezza.lubeinsights.platform.core.common.Utils;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.common.metadata.ZKUtil;
import com.saggezza.lubeinsights.platform.core.serviceutil.*;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class manages all the data stores
 */
public class DataStoreManager extends PlatformService {

    public static final Logger logger = Logger.getLogger(DataStoreManager.class);

    private static ServiceConfig config;
    private String tenantName;
    private String applicationName;
    private StorageEngine storageEngine;  // let's just use one for now
    private ConcurrentHashMap<String, DataStore> allDataStores; // for this tenant and application

    static {
        config = ServiceConfig.load();
    }

    /**
     * load all data stores and derive specs
     */
    public final void init(ServiceConfig config) {

        String engineSpec = config.get("storage-engine");
        tenantName = config.get("tenant");
        applicationName = config.get("application");
        if (engineSpec != null) {
            String[] typeLocation = engineSpec.split(",");
            storageEngine = new StorageEngine(typeLocation[0].trim(), typeLocation[1].trim());
        }

        // load all data stores
        allDataStores = DataStoreCatalog.getDataStores(tenantName,applicationName);
        // set up all derivations
        ConcurrentHashMap<String,String> deriveSpecs = DataStoreCatalog.getAllDeriveSpecs(tenantName,applicationName);
        for (String key: deriveSpecs.keySet()) {
            String[] fromAndTo = key.split("->");
            TemporalStore from = (TemporalStore)allDataStores.get(fromAndTo[0]);
            TemporalStore to = (TemporalStore)allDataStores.get(fromAndTo[1]);
            from.derive(DeriveSpec.fromJson(deriveSpecs.get(key)),to); // set up derive info in from and to
        }
    }

    public DataStoreManager() {super(ServiceName.DATASTORE_MANAGER);}

    public ServiceResponse processRequest(ServiceRequest request) {
        return null;  // TODO
    }

    /**
     * get the data store for the name
     * @param name
     * @return
     */
    public final DataStore getDataStore(String name) {
        return allDataStores.get(name);
    }


    /**
     * called from platform app
     * add a new data store to catalog
     * This only add a new data store without new derivation info. Derivation is added by another call (deriveDataStore) from platform app
     * // TODO: create underlying representation in HBase ?
     * @param name
     * @param dataModel
     * @param storageEngine
     * @param isTemporal
     */
    public DataStore newDataStore(String name, DataModel dataModel, StorageEngine storageEngine, boolean isTemporal, boolean force) {
        if (allDataStores.containsKey(name) && !force) {
            logger.error("Data Store "+name+" already exits. Cannot add it.");
            return null;
        }
        else {
            // create one and add to catalog
            DataStore store=null;
            if (isTemporal) {
                store = new TemporalStore(name, dataModel, storageEngine);
            }
            else {
                store = new ReferenceStore(name, dataModel, storageEngine);
            }
            // add it to catalog
            DataStoreCatalog.addDataStore(tenantName, applicationName, store);
            allDataStores.put(name, store);
            return store;
        }
    }


    /**
     * called from apps
     * derive a new data store, and save it and the driveSpec to catalog
     * @param fromName
     * @param deriveSpec
     * @return
     */
    public TemporalStore deriveDataStore(String fromName, String toName, DeriveSpec deriveSpec) {
        TemporalStore from = (TemporalStore) getDataStore(fromName);
        DataModel dm = deriveSpec.deriveDataModel(from.getDataModel());
        TemporalStore to = (TemporalStore) newDataStore(toName, dm, storageEngine, true, true);
        // set up derivation in both data stores
        from.derive(deriveSpec,to);
        // then add to catalog
        DataStoreCatalog.addDeriveSpec(tenantName, applicationName, fromName, toName, deriveSpec);
        return to;
    }


    /**
     * remove a data store from catalog after closing it
     * This method does not really purge the data or schema in the underlying storage engine, however.
     * @param name
     */
    public final void removeDataStore(String name) throws IOException {
        DataStore store = allDataStores.remove(name);
        store.close();
        DataStoreCatalog.removeDataStore(tenantName, applicationName, name);
    }


    /**
     * read from a flat file of line records as described in dataModel
     * @param filePath
     * @param dataModel
     * @return
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static final ArrayList<DataElement> readFromFile(String filePath, DataModel dataModel) throws FileNotFoundException,IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {

            Map<String,DataModel> hm = dataModel.getMap();
            int count = 0;
            String[] column = new String[hm.size()];
            DataType[] type = new DataType[hm.size()];
            for (String key: hm.keySet()) {
                column[count] = key;
                type[count++] = hm.get(key).getDataType();
            }
            ArrayList<DataElement> result = new ArrayList<DataElement>();
            for (String line = reader.readLine(); line != null; line = reader.readLine() ) {
                if (line.startsWith("#") || line.isEmpty()) {
                    continue;
                }
                String[] fields = line.split(",");
                // construct data element
                TreeMap rec = new TreeMap<String, DataElement>();
                for (int i = 0; i < fields.length; i++) {
                    switch (type[i]) {
                        case TEXT:
                            rec.put(column[i], new DataElement(DataType.TEXT, fields[i].trim()));
                            break;
                        case NUMBER:
                            rec.put(column[i], new DataElement(DataType.NUMBER, Integer.valueOf(fields[i].trim())));
                            break;
                        case DATETIME:
                            rec.put(column[i], new DataElement(DataType.DATETIME, Utils.CurrentDateSecond.parse(fields[i].trim())));
                            break;
                    }
                }
                result.add(new DataElement(rec));
            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }


    public static final void main(String[] args) {

        try {

            DataStoreManager mgr = new DataStoreManager();
            mgr.init(config); // load all data stores and derive specs
/*
            // define data mode dm1: <userName:text, gender:text, age:number, createDate:number>
            TreeMap<String, DataModel> tm1 = new TreeMap<String, DataModel>();
            tm1.put("userName", new DataModel(DataType.TEXT));
            tm1.put("gender", new DataModel(DataType.TEXT));
            tm1.put("age", new DataModel(DataType.NUMBER));
            tm1.put("createDate", new DataModel((DataType.NUMBER)));
            DataModel dm1 = new DataModel(tm1);
*/
            // define data mode dm2: <gender:text, createDate:number>
/*
            TreeMap<String, DataModel> tm2 = new TreeMap<String, DataModel>();
            tm2.put("gender", new DataModel(DataType.TEXT));
            tm2.put("createDate", new DataModel((DataType.NUMBER)));
            tm2.put("userCount", new DataModel((DataType.NUMBER)));
            DataModel dm2 = new DataModel(tm2);
*/

/*
            // define deriveSpec
            String filterName = null;  // no filter
            String aggFields = null;  // no field to aggregate
            String aggFieldAlias = "[userCount]";
            String groupByFields = "[gender]";
            String temporalKeys = "[createDate]";
            String windowFunction = null;  // don't transfer window value
            String windowName = "createDate";
            DeriveSpec deriveSpec = new DeriveSpec(filterName,aggFields,aggFieldAlias,
                    groupByFields,temporalKeys,windowFunction,windowName);

            // derive data model
            DataModel dm2 = deriveSpec.deriveDataModel(dm1);

            // define store1
            TemporalStore store1 = (TemporalStore) mgr.newDataStore("store1", dm1, mgr.storageEngine, true, true);
            // define store2
            TemporalStore store2 = mgr.deriveDataStore("store1","store2",deriveSpec);
*/
            TemporalStore store1 = (TemporalStore)mgr.getDataStore("store1");
            TemporalStore store2 = (TemporalStore)mgr.getDataStore("store2");
            // show them
            System.out.println(store1.serialize());
            System.out.println(store2.serialize());


            // send elements to store1 (and propagate to store2)
            ArrayList<DataElement> al = readFromFile(args[0],store1.getDataModel());
            for (DataElement e: al) {
                store1.addDataElement(e);
            }
            // use empty as end of batch token
            //store1.addDataElement(DataElement.EMPTY);

            store1.close();
            store2.close();

            // temporary. will use PlatformService.stop later
            ZKUtil.close(); // TODO: why it takes so long?

            System.out.println("exit");
            System.exit(0);


        } catch (Exception e) {e.printStackTrace();}

    }



}
