package com.saggezza.lubeinsights.platform.core.dataengine;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;

/**
 * @author : Albin
 */
public class IOUtils {

    public static String serializeRecord(DataElement record){
        return scala.collection.immutable.List.fromArray(record.allValues().toArray()).mkString(",");
    }
    //TODO - default serialization to contain data type also
    public static DataElement parseRecord(String recordText){
        return DataElement.FromArray(recordText.split(","), DataType.TEXT);
    }

}
