package com.saggezza.lubeinsights.platform.core.serviceutil;

/**
 * Created by chiyao on 7/17/14.
 */

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import com.saggezza.lubeinsights.platform.core.common.metadata.ZKUtil;

/**
 * This class defines where each service is running.
 * It is persistent information tracking each platform service's state
 */
public class ServiceCatalog {

    public static final Logger logger = Logger.getLogger(ServiceCatalog.class);
    public static final ServiceConfig config = ServiceConfig.load();


    private static ConcurrentHashMap<ServiceName,String> localCatalog =  new ConcurrentHashMap<ServiceName,String>();

    /**
     * register a service when it turns on
     * @param name
     * @param address
     */
    public static void serviceOn(ServiceName name, String address) {

        if (!ZKUtil.isZookeeperAvailable()) {
            localCatalog.put(name,address);
        }
        else {
            String nodePath = zkPath(name);
            // see if there is collision first
            String data = ZKUtil.zkGet(nodePath);
            if (data !=null) {
                String[] s = data.split(",");
                if (!s[1].equals("off")) { // there is data like: <ts>,<address>
                    throw new RuntimeException("Service " + name + " is already on "+s[1]);
                }
            }
            ZKUtil.zkSet(nodePath, zkDataForServiceOn(address));
            System.out.println("Registering service "+name+" on "+address);
            logger.info("Registering service "+name+" on "+address);
        }
    }

    /**
     * de-register a service when it turns off
     * @param name
     */
    public static void serviceOff(ServiceName name) {
        if (!ZKUtil.isZookeeperAvailable()) {
            localCatalog.remove(name);
        }
        else {
            try {
                String nodePath = zkPath(name);
                ZKUtil.zkSet(nodePath, zkDataForServiceOff());

            } catch (Exception e) {
                e.printStackTrace();
                logger.trace("serviceOff() Error: " + name, e);
            }
        }
    }

    /**
     * This will clear the registry data for the corresponding node in zk
     * @param name
     */
    public static final void clearRegistry(ServiceName name) {
        serviceOff(name);
    }

    public static final boolean isServiceOn(ServiceName name) {
        if (!ZKUtil.isZookeeperAvailable()) {
            return localCatalog.containsKey(name);
        }
        else {
            try {
                String nodePath = zkPath(name);
                if (!ZKUtil.exists(nodePath)) {
                    return false; // not found means off
                }
                else {
                    String data = new String(ZKUtil.getData(nodePath));
                    return (!data.equalsIgnoreCase("NA") && !data.contains("off "));
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.trace("serviceOn() Error: " + name, e);
                return false;  // error means off
            }
        }
    }


    /**
     * find the address for the service and cache the result
     * @param serviceName
     * @return service address or null if not found
     */
    public static final String findAddress(ServiceName serviceName) {

        if (!ZKUtil.isZookeeperAvailable()) { // zoo keeper not available, use localCatalog
            String address = localCatalog.get(serviceName);
            if (address != null) {
                return address;
            }
            switch (serviceName) {
                // hard coded catalog for testing
                case WORKFLOW_ENGINE:
                    return "localhost:8081";
                case DATA_ENGINE:
                    return "localhost:8082";
                default:
                    return null;
            }
        }
        else {  // find it from zookeeper
            try {
                String data = ZKUtil.getData(zkPath(serviceName));
                if (data != null) {
                    return zkData2Address(data);
                } else {
                    return null;
                }
            } catch (Exception e) {
                e.printStackTrace();
                logger.trace("ZooKeeper getData() error",e);
                return null;
            }
        }
    }

    /**
     * This representation is under the assumption that each service component has only ONE instance running
     * @param address
     * @return  <ts>,address
     */
    private static final String zkDataForServiceOn(String address) {
       return new StringBuilder(String.valueOf(System.currentTimeMillis())).append(",").append(address).toString();
     }

    /**
     *
     * @return  <ts>,off
     */
    private static final String zkDataForServiceOff() {
        return new StringBuilder(String.valueOf(System.currentTimeMillis())).append(",off").toString();
    }

    public static final String zkPath(ServiceName serviceName) {
        return new StringBuilder("/").append(getTenant()).append("/servicecatalog/")
                .append(getApplication()).append("/")
                .append(serviceName).toString();
    }

    /**
     * return a correct address representation, or null if off or not in address form
     * data is either <ts>,<address>   or     <ts>,off
     * @param data
     * @return
     */
    private static final String zkData2Address(String data) {
        String[] pair = data.split(",");
        if (pair.length>1 && pair[1] != null && !pair[1].equalsIgnoreCase("off")) {
            return pair[1];
        }
        else {
            return null;
        }
    }


    public static final String getTenant() {
        String tenant = config.get("tenant");
        return (tenant==null ? "tenant0" : tenant);
    }

    public static final String getApplication() {
        String application = config.get("application");
        return (application == null ? "application0" : application);
    }

    /**
     * load catalog from property
     * @param prop
     */
    public static void load(Properties prop) {
        Map map = prop;
        for(Object key : map.keySet()){
            String val = (String) map.get(key);
            localCatalog.put(ServiceName.valueOf(key.toString()), val);
        }
    }


    public static final void main(String[] args) {

        try {
            if (args != null && args.length == 1){  // clear registry for a service
                clearRegistry(ServiceName.valueOf(args[0]));
            }
            else { // just test
                serviceOn(ServiceName.COLLECTION_ENGINE, "localhost:1234");
                serviceOff(ServiceName.COLLECTION_ENGINE);
                ZKUtil.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
