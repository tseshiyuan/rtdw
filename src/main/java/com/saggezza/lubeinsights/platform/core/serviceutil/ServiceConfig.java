package com.saggezza.lubeinsights.platform.core.serviceutil;

/**
 * Created by chiyao on 7/24/14.
 */

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ServiceConfig {

    Properties prop;

    public ServiceConfig(Properties prop) {
        this.prop = prop;
    }

    public final String getHost() {return prop.getProperty("host");}

    public final String getPort() {return prop.getProperty("port");}

    public final String getAddr() {
        String host = getHost();
        String port = getPort();
        if (host != null & port != null) {
            return host+":"+port;
        }
        else {
            return null;
        }
    }

    public final String get(String name) {return prop.getProperty(name);}

    public final String getSparkMaster(){
        return prop.getProperty("spark-master");
    }

    public final String getSparkOutputFolder(){
        return prop.getProperty("spark-outputDir");
    }


    public final String getZKAddr() {
        return prop.getProperty("zookeeper");
    }

    public static final ServiceConfig load() {
        Properties prop = new Properties();
        try {
            String confFileName = System.getProperty("service.conf");
            //prop.load(ServiceConfig.class.getClassLoader().getResourceAsStream(configFileName));
            prop.load(new FileInputStream(confFileName));
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
        return new ServiceConfig(prop);
    }

}
