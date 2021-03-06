package com.saggezza.lubeinsights.platform.core.serviceutil;

/**
 * Created by chiyao on 7/30/14.
 */
public class ResourceManager {
    private static final String tempDir = ServiceConfig.load().get("tempDir");

    //TODO - change this to accomodate where to allocate file
    public static final String allocateFile(String label) {
        return new StringBuilder(tempDir)
                    .append("/")
                    .append(label)
                    .append(".")
                    .append(System.currentTimeMillis())
                    .toString();
    }

    /**
     *
     * @param label
     * @return path name for hdfs file
     */
    public static final String allocateHDFS(String label) {
        return null; //TODO
    }

}
