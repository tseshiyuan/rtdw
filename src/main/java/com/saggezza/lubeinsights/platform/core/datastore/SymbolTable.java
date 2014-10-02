package com.saggezza.lubeinsights.platform.core.datastore;

import com.google.common.primitives.UnsignedInteger;

import java.util.concurrent.ConcurrentHashMap;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by chiyao on 9/22/14.
 */

/**
 * SymbolTable keeps all the seen symbol and allows no more than 2**32 symbols
 */
public class SymbolTable {

    private static ConcurrentHashMap<String,UnsignedInteger> hm = new ConcurrentHashMap<String,UnsignedInteger>();
    private static UnsignedInteger counter = UnsignedInteger.ZERO;

    public static final UnsignedInteger getId(String key) {
        UnsignedInteger value = hm.get(key);
        if (value != null) {
            return value;
        }
        return createId(key);
    }

    private static synchronized UnsignedInteger createId(String key) {
        if (hm.containsKey(key)) {
            return hm.get(key);
        }
        UnsignedInteger value = counter;
        hm.put(key,value);
        counter = counter.plus(UnsignedInteger.ONE);
        return value;
    }

}
