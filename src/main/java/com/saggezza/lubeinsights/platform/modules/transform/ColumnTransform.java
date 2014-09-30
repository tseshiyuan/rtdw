package com.saggezza.lubeinsights.platform.modules.transform;

import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElementTypeError;
import com.saggezza.lubeinsights.platform.core.common.Params;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.module.Function;
import com.saggezza.lubeinsights.platform.core.common.modules.Modules;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author : Albin
 *
 * Applies any specified function to a specific column in the data.
 */
public class ColumnTransform implements Function {

    private String name;
    private int index = -1;
    private Function function;

    public ColumnTransform(){}

    private ColumnTransform(Params params){
        Object first = params.getFirst();
        if(first instanceof String){
            name = (String) first;
        }else if(first instanceof Integer) {
            index = (Integer) first;
        }
        String functionName = params.getSecond();
        this.function = Modules.function(functionName, params.remainingFrom(2));
    }

    @Override
    public DataElement apply(DataElement elem) throws DataElementTypeError {
        DataElement dataElement = elem;
        DataElement input = index == -1 ? dataElement.valueByName(name) : dataElement.valueAt(index);
        DataElement returned = this.function.apply(input);
        if(index == -1){
            dataElement.setValueNamed(name, returned);
        }else {
            dataElement.setValueAt(index, returned);
        }
        return dataElement;
    }

    @Override
    public DataModel apply(DataModel in) throws DataEngineExecutionException {
        DataModel target = index == -1 ? in.getMap().get(name) : in.getList().get(index);
        DataModel returned = this.function.apply(target);
        if(index == -1){
            TreeMap<String, DataModel> map = (TreeMap) in.getMap();
            map.put(name, returned);
            return new DataModel(map);
        }else {
            ArrayList<DataModel> list = in.getList();
            list.set(index, returned);
            return new DataModel(list);
        }
    }
}
