package com.saggezza.lubeinsights.platform.core.common;

import com.saggezza.lubeinsights.platform.core.common.metadata.ZKUtil;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by chiyao on 7/24/14.
 */

/**
 * This is for modules to exchange data/status
 * Environment is accessible for all localRun modules, so they are SHARED in the same workflow exec context.
 * Assumption is that any context referring to a named environment must run on the same JVM (e.g. a workflow instantiation)
 */
public class Environment {

    private String tenant;
    private String application;
    private String name; // environment name (for scoping purpose)

    private ConcurrentHashMap<String,String> env = new ConcurrentHashMap<String,String>();

    public Environment(String tenant, String application, String name) {
        this.tenant = tenant;
        this.application = application;
        this.name = name;
    }

    private final String zkPath(String varName) {
    return new StringBuilder("/").append(tenant).append("/environment/")
            .append(application).append("/")
            .append(name).append("/")
            .append(varName).toString();
    }

    /**
     * Set the value of varName to value, and update zk if it's different from what we see in cache.
     * @param varName
     * @param value
     */
    public final void setValue(String varName, String value) {
        String oldValue = env.get(varName);
        if (!oldValue.equals(value)) {
            env.put(varName, value);
            // save change to zookeeper
            String path = zkPath(varName);
            ZKUtil.zkSet(path, value); // each env variable has a zk entry
        }
    }

    /**
     * get value of varName from this environment
     * If available in cache, get it from cache.
     * If not, get it from zk.
     * If not in zk, return null
     * @param varName
     * @return value of varName, or null if never set
     */
    public final String getValue(String varName) {
        if (env.containsKey(varName)) {
            return env.get(varName);
        }
        else {
            String val = ZKUtil.zkGet(zkPath(varName));
            if (val != null) {
                env.put(varName, val);
            }
            return val;
        }
    }


}
