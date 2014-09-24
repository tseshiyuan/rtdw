package com.saggezza.lubeinsights.platform.core.common.metadata;

import com.saggezza.lubeinsights.platform.core.serviceutil.ServiceConfig;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;

/**
 * Created by chiyao on 8/7/14.
 */
public class ZKUtil {

    public static final Logger logger = Logger.getLogger(ZKUtil.class);
    public static final String ZK_ADDR_DEFAULT = "localhost:2181";
    public static String zkAddr;
    private static ZooKeeper zk = null; // zookeeper client


    static {
        try {
            zkAddr = ServiceConfig.load().getZKAddr();
            if (zkAddr == null) {
                zkAddr = ZK_ADDR_DEFAULT;
            }
            Watcher watcher = new Watcher() {
                public void process(WatchedEvent event) {
                    System.out.println(event.toString());  // TODO: handle watched event
                }
            };
            zk = new ZooKeeper(zkAddr, 3000, watcher);
            zk.getData("/", false, null);//Sample test of connection
        } catch (Exception e) {
            zk = null;
            e.printStackTrace();
            logger.trace("Zookeeper not available", e);
        }
    }

    public static final boolean isZookeeperAvailable() {
        return zk != null;
    }

    /**
     * create a node with the path and data (assuming this path does not exist)
     * If its parent does not exist, create it first recursively using a special data token
     * @param path
     * @param data
     */
    public static final void zkCreate(String path, byte[] data) throws KeeperException, InterruptedException {
        String parent = zkGetParent(path);
        if (parent != null && zk.exists(parent,false) == null) { // create parent recursively if it does not exist
            zkCreate(parent, "NA".getBytes());
        }
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }


    /**
     * set a node at the path with data
     * if the node does not exist, create it with the data
     * @param path
     * @param data
     */
    public static final void zkSet(String path, String data) throws RuntimeException {
        try {
            if (zk.exists(path,false) == null) {
                zkCreate(path, data.getBytes());
            }
            else {
                zk.setData(path, data.getBytes(), -1); // version -1 matches any version in zk
            }
        } catch (Exception e) {
            logger.trace("ZooKeeper error",e);
            throw new RuntimeException(e);
        }
    }

    /**
     * get the data for the node at path
     * @param path
     * @return data for node at oath, or null if node does not exist
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static final String zkGet(String path) throws RuntimeException {
        try {
            if (zk.exists(path, false) == null) {
                return null;
            } else {
                return new String(zk.getData(path, false, null));
            }
        } catch (Exception e) {
            logger.trace("ZooKeeper error",e);
            throw new RuntimeException(e);
        }
    }


    public static final boolean exists(String path) throws KeeperException, InterruptedException {
        return (zk.exists(path,false) != null);
    }

    /**
     * get data for the node at path
     * @param path
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static final String getData(String path) throws RuntimeException {
        try {
            byte[] data = zk.getData(path, false, null);
            return (data == null ? null : new String(data));
        } catch (Exception e) {
            logger.trace("ZooKeeper error",e);
            throw new RuntimeException(e);
        }
    }


    /**
     *
     * @param path
     * @return the parent path, or null if path is "/"
     */
    public static final String zkGetParent(String path) {
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash == 0) {
            return null;
        }
        else {
            return path.substring(0,lastSlash);
        }
    }

    /**
     * called when platform service shut down and de-register from service catalog
     */
    public static final void close() {
        try {
            if (zk != null) {
                zk.close();
            }
        } catch (Exception e) {
            logger.trace("ZKUtil Error", e);
        }
    }




}
