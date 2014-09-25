package com.saggezza.lubeinsights.platform.core.datastore.temporal;

import java.util.Arrays;

/**
 * Created by chiyao on 9/22/14.
 */

/**
 * just a wrapper of byte[]
 */
public class ByteArray {

    private byte[] value;

    public ByteArray(byte[] value) {this.value=value;}

    @Override
    public final boolean equals(Object bytes) {
        if (! (bytes instanceof ByteArray)) {
            return false;
        }
        return Arrays.equals(value, ((ByteArray) bytes).value);
    }

    @Override
    public final int hashCode() {
        return Arrays.hashCode(value);
    }

}
