package com.saggezza.lubeinsights.platform.core.datastore;

import java.util.Arrays;

/**
 * Created by chiyao on 9/22/14.
 */

/**
 * just a wrapper of byte[]
 */
public class ByteArray implements Comparable<ByteArray> {

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

    @Override
    // byte wise comparison
    public final int compareTo(ByteArray ba) {
        for (int i=0; i<value.length && i<ba.value.length; i++) {
            if (value[i]<ba.value[i]) {
                return -1;
            }
            else if (value[i]>ba.value[i]) {
                return 1;
            }
        }
        if (value.length < ba.value.length) {
            return -1;
        }
        else if (value.length > ba.value.length) {
            return -1;
        }
        else {
            return 0;
        }
    }

}
