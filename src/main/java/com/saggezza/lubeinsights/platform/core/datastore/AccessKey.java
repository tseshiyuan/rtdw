package com.saggezza.lubeinsights.platform.core.datastore;

/**
 * Created by chiyao on 9/15/14.
 */
public class AccessKey {
    public enum type {HASH, NUMERIC};
    protected String keyName;
    protected type keyType;
}
