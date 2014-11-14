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

    //private static ConcurrentHashMap<String,UnsignedInteger> hm = new ConcurrentHashMap<String,UnsignedInteger>();
    private static ConcurrentHashMap<String,Integer> hm = new ConcurrentHashMap<String,Integer>();
    //private static UnsignedInteger counter = UnsignedInteger.ZERO;
    private static int counter = 0;

    public static final Integer getId(String key) {
        Integer value = hm.get(key);
        if (value != null) {
            return value;
        }
        return createId(key);
    }

    private static synchronized Integer createId(String key) {
        if (hm.containsKey(key)) {
            return hm.get(key);
        }
        int value = counter;
        hm.put(key,Integer.valueOf(value));
        //counter = counter.plus(UnsignedInteger.ONE);
        counter++;
        return value;
    }

}
