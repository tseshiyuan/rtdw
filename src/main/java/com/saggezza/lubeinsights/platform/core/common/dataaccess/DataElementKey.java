package com.saggezza.lubeinsights.platform.core.common.dataaccess;

import java.security.spec.KeySpec;
import java.util.ArrayList;
import java.util.List;

/**
 * @author : Albin
 */
public class DataElementKey implements Key{

    private final Selection keySpec;
    private final DataElement dataElement;

    DataElementKey(Selection keySpec, DataElement dataElement) {
        this.keySpec = keySpec;
        this.dataElement = dataElement;
    }

    @Override
    public Selection keySpec() {
        return keySpec;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataElementKey that = (DataElementKey) o;
//        System.out.println("[[["+dataElement.toString()+"]]][[["+that.dataElement.toString()+"]]]");
        if (dataElement != null ? !dataElement.allValues().toString().equals(that.dataElement.allValues().toString()) : that.dataElement != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = 0;
        result = 31 * result + (dataElement != null ? dataElement.allValues().toString().hashCode() : 0);
        return result;
    }
}
