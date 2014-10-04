package com.saggezza.lubeinsights.platform.core.common.datamodel;

/**
 * Created by chiyao on 8/21/14.
 */

import com.google.gson.Gson;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.FieldAddress;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.Selection;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngineExecutionException;
import com.saggezza.lubeinsights.platform.core.dataengine.ErrorCode;
import com.saggezza.lubeinsights.platform.core.datastore.Stats;
import com.saggezza.lubeinsights.platform.modules.datamodel.validator.ValidatorModule;

import java.util.*;
import java.util.function.Function;

/**
 * A DataModel object is a descriptor that describes a DataElement.
 * For example, how many fields does a record data element have? What are their data types and value constraints?
 * Using this descriptor, an application can validate/process a given DataElement properly.
 */
public class DataModel {

    public static final DataModel statsDataModel = createStatsModel();
    protected DataType dataType = null;
    protected ArrayList<DataModel> descList = null;
    protected TreeMap<String,DataModel> descMap = null;
    protected ArrayList<String> validatorNames = null;
    protected transient ArrayList<Function<DataElement, Boolean>> validators = null;
    protected transient Function<DataElement, Boolean> typeValidator = null;

    public DataModel() {
        initValidators();
    }

    public DataModel(DataType dataType) {
        this.dataType = dataType;
        initValidators();
    }

    public DataModel(ArrayList list) {
        descList = list;
        initValidators();
    }

    public DataModel(TreeMap map) {
        descMap = map;
        initValidators();
    }

    public DataModel select(Selection selection) throws DataEngineExecutionException {
        if(isPrimitive()){
            throw new DataEngineExecutionException(ErrorCode.PrimitiveDataElementNotExpected, "Primitive Data Element not expected for selection");
        }
        return selection.from(this);
    }

    public final boolean isPrimitive() {return dataType != null;}
    public final DataType getDataType() {return dataType;}

    public final boolean isList() {return descList != null;}
    public final ArrayList<DataModel> getList() {return descList;}


    public final boolean isMap() {return descMap != null;}
    public final Map<String,DataModel> getMap() {
        return descMap;
    }

    /**
     * return the type for field named field
     * @param field
     * @return null if not a map
     */
    public final DataType typeAt(String field) {
        if (isMap()) {
            return descMap.get(field).getDataType();
        }
        else {
            return null;
        }
    }

    /**
     * return the type for field at position index
     * @param index
     * @return null of not a list
     */
    public final DataType typeAt(int index) {
        if (isList()) {
            return descList.get(index).getDataType();
        }
        else {
            return null;
        }
    }

    public TreeSet<String> getAllFieldNames() {
        return getAllFieldNames(this);
    }


    /**
     * such as a.2.b or 0.a.b
     * @param dataModel
     * @return null if primitive type
     */
    private static final TreeSet<String> getAllFieldNames(DataModel dataModel) {
        if (dataModel.isPrimitive()) {
            return null;
        }
        if (dataModel.isMap()) {
            TreeSet<String> result = new TreeSet<String>();
            for (String field: dataModel.descMap.keySet()) {
                TreeSet<String> childNames = getAllFieldNames(dataModel.descMap.get(field));
                if (childNames != null) {
                    for (String cn: childNames) {
                        result.add(field+"."+cn);
                    }
                }
            }
            return result;
        }
        if (dataModel.isList()) {
            TreeSet<String> result = new TreeSet<String>();
            for (int i=0; i<dataModel.descList.size(); i++) {
                TreeSet<String> childNames = getAllFieldNames(dataModel.descList.get(i));
                if (childNames != null) {
                    for (String cn: childNames) {
                        result.add(i+"."+cn);
                    }
                }
            }
            return result;
        }
        return null;
    }

    /**
     * add typeValidator to validators
     */
    protected void initValidators() {
        if (validators == null) {
            validators = new ArrayList<Function<DataElement, Boolean>>();
        }
        if (typeValidator == null) {
            typeValidator = (e)-> validateType(e);
        }
        validators.add(typeValidator);
    }

    public ArrayList<DataModel> allValues(){
        ArrayList<DataModel> all = new ArrayList<>();
        if(isPrimitive()){
            all.add(this);
        }else if(isList()){
            all.addAll(getList());
        }else {
            all.addAll(getMap().values());
        }
        return all;
    }

    /**
     * recursively validate that the element has the same type as described by this dataaccess model
     * @param element
     * @return true iff element has the type as described by this dataaccess model
     */
    protected boolean validateType(DataElement element) {

        // null element does not validate
        if (element==null) {
            return false;
        }
        // validate primitive element
        DataType type = element.getTypeIfPrimitive();
        if (type != null) {
            return dataType==type;
        }
        // if it's a list, validate its elements recursively
        ArrayList<DataElement> list = element.asList();
        if (list != null) {
            if (descList==null) {
                return false;
            }
            int modelSize = descList.size();
            int eltSize = list.size();
            if (modelSize != eltSize) {
                return false;
            }
            for (int i=0; i<modelSize; i++) {
                if (!descList.get(i).validateType(list.get(i))) {
                    return false;
                }
            }
            return true;
        }
        // if it's a map, validate its elements recursively
        Map<String,DataElement> map = element.asMap();
        if (map != null) {
            if (descMap==null) {
                return false;
            }
            for (String fieldName: descMap.keySet()) {
                if (!descMap.get(fieldName).validateType(map.get(fieldName))) {
                    return false;
                }
            }
            return true;
        }
        // nothing validates
        return false;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if(isPrimitive()){
            return "{type : "+dataType+"}";
        }else if(isList()){
            builder.append("[");
            for(int i=0; i < descList.size(); i++){
                builder.append(descList.get(i).toString());
                builder.append(",");
            }
            return builder.substring(0, (builder.length() -1) ) + "]";
        }else{
            for(HashMap.Entry<String, DataModel> each : descMap.entrySet()){
                builder.append("{ "+each.getKey()+" : "+each.getValue().toString() + " } \n");
            }
            return builder.toString();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DataModel dataModel = (DataModel) o;

        if (dataType != dataModel.dataType) return false;
        if (descList != null ? !descList.equals(dataModel.descList) : dataModel.descList != null) return false;
        if (descMap != null ? !descMap.equals(dataModel.descMap) : dataModel.descMap != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = dataType != null ? dataType.hashCode() : 0;
        result = 31 * result + (descList != null ? descList.hashCode() : 0);
        result = 31 * result + (descMap != null ? descMap.hashCode() : 0);
        return result;
    }

    /**
     * add validators by name in module classes, and also add to validatorNames
     * @param className
     */
    public final void addValidator(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class validatorClass = Class.forName("com.saggezza.lubeinsights.platform.modules.datamodel.validator."+className);
        ValidatorModule validator = (ValidatorModule) validatorClass.newInstance();
        validators.add((e)->validator.validate(e));
        if (validatorNames==null) {
            validatorNames = new ArrayList<String>();
        }
        validatorNames.add(className);
    }

    /**
     * add validators by name in module classes, without adding to validatorNames (as it's there already)
     * @param className
     */
    protected final void copyToValidators(String className) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class validatorClass = Class.forName("com.saggezza.lubeinsights.platform.modules.datamodel.validator." + className);
        ValidatorModule validator = (ValidatorModule) validatorClass.newInstance();
        validators.add((e) -> validator.validate(e));
    }

    public final void addValidator(Function<DataElement,Boolean> validator) {
        validators.add(validator);
    }

    public final boolean validate(DataElement element) {
        for (Function<DataElement,Boolean> validator: validators) {
            if (!validator.apply(element)) {
                return false;
            }
        }
        return true;
    }

    public final String toJson() {
        return new Gson().toJson(this);
    }

    public static final DataModel fromJson(String json) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        DataModel dataModel = new Gson().fromJson(json, DataModel.class);
        // add validator by names
        dataModel.setValidators();
        return dataModel;
    }

    /**
     * recursively set validators by validator names
     */
    private void setValidators() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        if (validatorNames != null) {
            for (String s : validatorNames) {
                copyToValidators(s);
            }
        }
        if (descList != null) {
            for (DataModel m: descList) {
                m.setValidators();
            }
        }
        if (descMap != null) {
            for (DataModel m: descMap.values()) {
                m.setValidators();
            }
        }
    }

    /**
     * a model holding 5 stats numbers
     * @return
     */
    private static final DataModel createStatsModel() {
        ArrayList<DataModel> al = new ArrayList<DataModel>();
        for (Stats s:Stats.values()) {
            al.add(new DataModel(DataType.NUMBER));
        }
        return new DataModel(al);
    }


    /**
     * universal field retriever using coordinates. Each coordinate is either a string key or a integer index
     * @param fieldAddress
     */
    public final DataModel getField(FieldAddress fieldAddress) {
        return get(fieldAddress.getCoordinate(),0);
    }

    /**
     * recursively access a field using the coordinates starting from startPos
     * @param coordinate
     * @param startPos
     * @return the data element at the coordinates starting from startPos
     */
    private final DataModel get(Object[] coordinate, int startPos) {
        if (startPos == coordinate.length) {
            return this;
        }
        else if (coordinate[startPos] instanceof Integer) {
            return descList.get((Integer)coordinate[startPos]).get(coordinate,startPos+1);
        }
        else {
            return descMap.get((String)coordinate[startPos]).get(coordinate,startPos+1);
        }
    }


    public static final void main(String[] args) {

        try {

            ArrayList<DataModel> list = new ArrayList<DataModel>();
            DataModel shortText = new DataModel(DataType.TEXT);
            shortText.addValidator("IsShortString");
            list.add(shortText);
            list.add(new DataModel(DataType.NUMBER));
            DataModel dataModel = new DataModel(list);
            // serialize
            String json = dataModel.toJson();
            System.out.println(json);
            // de-serialize
            dataModel = DataModel.fromJson(json);
            System.out.println(dataModel.toJson());
            // try to validate an element
            DataElement[] fields = new DataElement[] {new DataElement(DataType.TEXT,"test"), new DataElement(DataType.NUMBER,10)};
            DataElement record = new DataElement(fields);
            System.out.println("Validation result: " + dataModel.validate(record));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
