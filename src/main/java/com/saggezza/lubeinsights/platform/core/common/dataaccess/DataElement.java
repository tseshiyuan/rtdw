package com.saggezza.lubeinsights.platform.core.common.dataaccess;

import com.google.common.collect.Lists;
import com.google.gson.*;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataType;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngine;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Created by chiyao on 7/25/14.
 */

/**
 * DataElement is the unit of dataaccess in DataSet
 * It can be an object of primitive types (TEXT, NUMBER, DATETIME), or a structure, which is either a list or a map of DataElement recursively.
 * A DataElement can be validated by a DataModel to ensure it has the right type, structure and content format in an application context.
 */
public class DataElement implements Serializable {

    protected DataType dataType = null;    // primitive types: TEXT, NUMBER and DATETIME
    protected Object value = null;         // primitive values of type String, Number or DateTime
    protected ArrayList<DataElement> list = null;     // available only when this DataElement is a list
    protected TreeMap<String,DataElement> map = null; // available only when this DataElement is a map
    private static final String dataKey = "_d";
    private static final String valueKey = "_v";

    /**
     * create a DataElement of primitive type
     * @param dataType
     * @param value
     */
    public DataElement(DataType dataType, Object value) {
        this.dataType = dataType;
        this.value = value;
    }

    /**
     * create a list DataElement
     * @param list
     */
    public DataElement(ArrayList<DataElement> list) {
        this.list = list;
    }

    /**
     * create a map DataElement
     * @param list
     */
    public DataElement(DataElement[] list) {
        this.list = new ArrayList<DataElement>();
        for (DataElement e: list) {
            this.list.add(e);
        }
    }

    boolean isPrimitive(){
        return value != null && dataType != null;
    }

    boolean isList(){
        return list != null && !list.isEmpty();
    }

    boolean isMap(){
        return map != null && !map.isEmpty();
    }

    public DataElement select(Selection selection){
        if(isPrimitive()){
            throw new RuntimeException("Primitive dataset. Cannot select.");
        }
        return selection.from(this);
    }

    public DataElement(HashMap<String,DataElement> map) {
        TreeMap<String, DataElement> treeMap =  new TreeMap<>();
        treeMap.putAll(map);
        this.map = treeMap;
    }

    /**
     * @return a list of DataElement, or null if it's not a list
     */
    public final ArrayList<DataElement> asList() {
        return list;
    }

    /**
     * @return a map of String to DataElement, or null if it's not a map
     */
    public final HashMap<String,DataElement> asMap() {
        HashMap<String, DataElement> map = new HashMap<>();
        map.putAll(this.map);
        return map;
    }

    /**
     * @return its primitive data type, or null if it's not a primitive
     */
    public final DataType getTypeIfPrimitive() {
        return dataType;
    }

    /**
     * @return its text value, or null if it's not a TEXT
     */
    public String asText() {
        if (dataType==DataType.TEXT) {
            return (String) value;
        }
        else {
            return null;
        }
    }

    /**
     * @return its text value, or null if it's not a NUMBER
     */
    public Number asNumber() {
        if (dataType==DataType.NUMBER) {
            return (Number) value;
        }
        else {
            return null;
        }
    }

    /**
     * @return its text value, or null if it's not a DATETIME
     */
    public Date asDateTime() {
        if (dataType==DataType.DATETIME) {
            return (Date) value;
        }
        else {
            return null;
        }
    }

    /**
     * generate coordinates for a list of addresses
     * @param address comma separated coordinates, each coordinate is a dot separated value
     *        address is like user.phoneNumber[0].areaCode,product.serialNum
     *        or in case of flat record, it can be userId,productId,serialNum
     * @return [[user,phoneNumber,0,areaCode],[product,serialNum] in case 1
     *        or [[userId],[productId],[serialNum]] in case 2
     */
    public static final Object[][] generateCoordinates(String address) {
        String[] addresses = address.split(",");
        Object[][] result = new Object[addresses.length][];
        for (int i=0; i<addresses.length; i++) {  // transform each address to coordinate of form Object[]
            String[] parts = address.split(".");
            ArrayList coordinates = new ArrayList();
            for (String part : parts) {
                if (part.endsWith("]")) { // it is list access like A[n]
                    int bracketPos = part.indexOf('[');
                    if (bracketPos > 0) {
                        coordinates.add(part.substring(0, bracketPos));
                    }
                    coordinates.add(Integer.valueOf(part.substring(bracketPos + 1, part.length() - 1)));
                } else {  // it's map access
                    coordinates.add(part);
                }
            }
            result[i] = coordinates.toArray();
        }
        return result;
    }

    /**
     * universal field retriever using an address, which is a string representation of coordinates
     * @param address
     */
    public final DataElement get(String address) {
        return get(generateCoordinates(address),0);
    }

    /**
     * universal field retriever using coordinates. Each coordinate is either a string key or a integer index
     * @param coordinates
     */
    public final DataElement get(Object[] coordinates) {
        return get(coordinates,0);
    }

    /**
     * recursively access a field using the coordinates starting from startPos
     * @param coordinates
     * @param startPos
     * @return the data element at the coordinates starting from startPos
     */
    private final DataElement get(Object[] coordinates, int startPos) {
        if (startPos == coordinates.length) {
            return this;
        }
        else if (coordinates[startPos] instanceof Integer) {
            return list.get((Integer)coordinates[startPos]).get(coordinates,startPos+1);
        }
        else {
            return map.get((String)coordinates[startPos]).get(coordinates,startPos+1);
        }
    }

    /**
     * get an array of fields bases on an array of addresses
     * @param addresses
     * @return
     */
    public DataElement[] getElements(Object[][] addresses) {
        DataElement[] result = new DataElement[addresses.length];
        for (int i=0; i<addresses.length; i++) {
            result[i] = get(addresses[i]);
        }
        return result;
    }

    /**
     * get a deep copy of this
     * @return
     */
    public DataElement clone() {
        if (dataType != null) { // primitive type
            return new DataElement(dataType, value);
        }
        if (list != null) { // it's a list
            ArrayList<DataElement> al = new ArrayList<DataElement>();
            for (DataElement e : list) {
                al.add(e.clone());
            }
            return new DataElement(al);
        }
        if (map != null) { // it's a map
            HashMap<String,DataElement> hm = new HashMap<String,DataElement>();
            for (String k: map.keySet()) {
                hm.put(k, map.get(k).clone());
            }
            return new DataElement(hm);
        }
        return null; // can't clone unknown type
    }

    /**
     * change this data element into a list (in-place update)
     * @param list
     */
    public void setToList(DataElement[] list) {
        dataType = null;
        value = null;
        map = null;
        this.list = new ArrayList<DataElement>();
        for (DataElement e:list) {
            this.list.add(e);
        }
    }


    /**
     * in-place update
     * add a float to the data element value if this is a NUMBER
     * @param value
     */
    public final void addValue(float value) {
        if (dataType == DataType.NUMBER) {
            this.value = (float) this.value + value;
        }
        // else no-op
    }

    /**
     *
     * @return a short representation of this data element (without type tag, comma delimited)
     */
    public final String toString() {
        if(dataType != null) {
            switch (dataType) {
                case TEXT:
                    return (String) value;
                case NUMBER:
                    return String.valueOf((Number) value);
                case DATETIME:
                    return ((Date) value).toString();
            }
        }
        if (list != null) {
            StringBuilder sb = new StringBuilder("");
            for (DataElement e: list) {
                sb.append(e.toString()).append(",");
            }
            sb.setLength(sb.length()-1);
            sb.append("");
            return sb.toString();
        }else if (map != null) {
            StringBuilder sb = new StringBuilder("{");
            List<String> keys = Lists.newArrayList(map.keySet());
            Collections.sort(keys);//To make the order consistent
            for (String key : keys) {
                sb.append(key).append(":").append(map.get(key).toString()).append(",");
            }
            sb.setLength(sb.length()-1);
            sb.append("}");
            return sb.toString();
        }else {
            return null;
        }
    }

    public DataElement valueByName(String name){
        return map.get(name);
    }

    public DataElement valueAt(int index){
        validateList();
        return list.get(index);
    }

    private void validateList(){
        if (list==null) {
            throw new DataElementTypeError("Cannot set value for index on a DataElement that's not a list");
        }
    }

    private void validateMap(){
        if (map==null) {
            throw new DataElementTypeError("Cannot set value for name on a DataElement that's not a map");
        }
    }

    /**
     * applicable only to list dataaccess elements
     * @param index
     * @param elt
     * @throws DataElementTypeError if it's not a list
     */
    public void setValueAt(int index, DataElement elt) throws DataElementTypeError {
        validateList();
        list.set(index, elt);
    }

    /**
     * applicable only to map dataaccess elements
     * @param name
     * @param elt
     * @throws DataElementTypeError if it's not a map
     */
    public void setValueNamed(String name, DataElement elt) throws DataElementTypeError {
        validateMap();
        map.put(name,elt);
    }

    public Key key(Selection fromKey) {
        return new DataElementKey(fromKey, select(fromKey));
    }

    public ArrayList<DataElement> allValues() {
        ArrayList<DataElement> ret = new ArrayList<>();
        if(isPrimitive()){
            ret.add(this);
        }else if(isList()){
            ret.addAll(list);
        }else {
            ret.addAll(map.values());
        }
        return ret;
    }

    public static DataElement FromArray(Object[] arr, DataType dataType){
        ArrayList<DataElement> elem = new ArrayList<>();
        for(Object each : arr){
            elem.add(new DataElement(dataType, each));
        }
        return new DataElement(elem);
    }

    public int length() {
        if(isPrimitive()){
            return 1;
        }else if(isList()){
            return list.size();
        }else {
            return map.size();
        }
    }



    public String serialize(){
        return new Gson().toJson(asJson());

    }

    public static void main(String[] args) {
        String in = "{\"_d\":\"list\",\"_v\":[{\"_d\":\"1\",\"_v\":\"2008\"},{\"_d\":\"1\",\"_v\":\"WN\"},{\"_d\":\"2\",\"_v\":\"12110.0\"},{\"_d\":\"1\",\"_v\":\"2008\"},{\"_d\":\"2\",\"_v\":\"12110.0\"}]}";
        DataElement element = parseSerialized(in);
        element.key(new Selection(0, 1));
        System.out.println(element);
    }

    public static DataElement parseSerialized(String json){
        JsonElement jsonElement = new JsonParser().parse(json);
        if(jsonElement.isJsonObject()){
            return fromJson(jsonElement);
        }else {
            throw new RuntimeException("Not valid serialized Json");
        }
    }

    private static DataElement fromJson(JsonElement jsonElement){
        JsonObject in = (JsonObject) jsonElement;
        String dataKey = in.get(DataElement.dataKey).getAsString();
        JsonElement valuePart = in.get(valueKey);
        if("list".equals(dataKey)){
            ArrayList<DataElement> elems = new ArrayList<DataElement>();
            JsonArray vals = (JsonArray) valuePart;
            for(int i=0; i < vals.size(); i++){
                JsonElement each = vals.get(i);
                elems.add(fromJson(each));
            }
            return new DataElement(elems);
        }else if("map".equals(dataKey)){
            HashMap<String, DataElement> map = new HashMap<>();
            JsonObject vals = (JsonObject) valuePart;
            Set<Map.Entry<String, JsonElement>> entries = vals.entrySet();
            for(Map.Entry<String, JsonElement> each : entries){
                map.put(each.getKey(), fromJson(each.getValue()));
            }
            return new DataElement(map);
        }else {
            int ordinal = Integer.parseInt(dataKey);
            DataType type = DataType.ordinal(ordinal);
            Object deserialized = type.deserialize(valuePart.getAsString());
            return new DataElement(type, deserialized);
        }
    }

    private JsonElement asJson(){
        JsonObject root = new JsonObject();
        if(isPrimitive()){
            root.addProperty(dataKey, dataType.getOrdinal()+"");
            root.addProperty(valueKey, dataType.serialize(value));
        }else if(isList()){
            root.addProperty(dataKey, "list");
            JsonArray values = new JsonArray();
            root.add(valueKey, values);
            for(DataElement each : list){
                values.add(each.asJson());
            }
        }else {
            root.addProperty(dataKey, "map");
            JsonObject values = new JsonObject();
            root.add(valueKey, values);
            for(Map.Entry<String, DataElement> each : map.entrySet()){
                values.add(each.getKey(), each.getValue().asJson());
            }
        }
        return root;

    }

}
