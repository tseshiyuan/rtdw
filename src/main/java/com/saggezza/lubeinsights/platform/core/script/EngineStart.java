package com.saggezza.lubeinsights.platform.core.script;

import com.saggezza.lubeinsights.platform.core.collectionengine.CollectionEngine;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngine;
import com.saggezza.lubeinsights.platform.core.datastore.DataStoreManager;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceCatalog;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceConfig;
import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceName;
import com.saggezza.lubeinsights.platform.core.workflowengine.WorkFlowEngine;

import java.io.FileInputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author : Albin
 */
public class EngineStart {

    private static ServiceConfig config = ServiceConfig.load();
    private static final String WORKFLOW_ENGINE = config.get("WORKFLOW_ENGINE_NAME");
    private static final String DATA_ENGINE = config.get("DATA_ENGINE_NAME");
    private static final String COLLECTION_ENGINE = config.get("COLLECTION_ENGINE_NAME");
    private static final String DATASTORE_MANAGER = config.get("DATASTORE_MANAGER_NAME");

    public static void main(String[] args) {
        try{
            if (args.length != 1) {
                printUsage();
                System.exit(0);
            }
            String command = args[0];
            String[] componentArgs = new String[args.length-1];
            System.arraycopy(args,1,componentArgs,0,componentArgs.length);

            if (command.equalsIgnoreCase(WORKFLOW_ENGINE)) {
                String engineLocation = config.get(ServiceName.WORKFLOW_ENGINE.name());
                int port = Integer.parseInt(engineLocation.split(":")[1]);
                new WorkFlowEngine().start(port);
            }
            else if (command.equalsIgnoreCase(DATA_ENGINE)) {
                DataEngine.main(componentArgs);
            }
            else if (command.equalsIgnoreCase(COLLECTION_ENGINE)) {
                String engineLocation = config.get(ServiceName.COLLECTION_ENGINE.name());
                int port = Integer.parseInt(engineLocation.split(":")[1]);
                new CollectionEngine().start(port);
            }
            else if (command.equalsIgnoreCase(DATASTORE_MANAGER)) {
                String engineLocation = config.get(ServiceName.DATASTORE_MANAGER.name());
                int port = Integer.parseInt(engineLocation.split(":")[1]);
                new DataStoreManager().start(port);
            }
            System.out.println("Engine "+command+" started");

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static void printUsage(){
        System.out.println("Need component name and config file as arguments");
    }

}
