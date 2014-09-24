package com.saggezza.lubeinsights.platform.core.serviceutil;

/**
 * Created by chiyao on 7/30/14.
 */
public class ResourceManager {
    private static final String tempDir = ServiceConfig.load().get("tempDir");

    public static final String allocateFile(String label) {
        return tempDir+label+"."+System.currentTimeMillis(); // TODO: check name collision due to timestamp ?
    }

}
