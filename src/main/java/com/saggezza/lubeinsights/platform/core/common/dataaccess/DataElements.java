package com.saggezza.lubeinsights.platform.core.common.dataaccess;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author : Albin
 */
public class DataElements extends ArrayList<DataElement> {

    public DataElements() {
    }

    public DataElements(Collection<? extends DataElement> c) {
        super(c);
    }
}
