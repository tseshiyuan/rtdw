package com.saggezza.lubeinsights.platform.core.common;

import com.google.gson.*;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.*;

import static com.saggezza.lubeinsights.platform.core.common.GsonUtil.gson;

/**
 * @author : Albin
 */
public class Params implements Serializable{

    private Object[] values;
    private HashMap<String,Object> namedValues;

    public static final Params None = new Params();

    private Params(Object... values) {
        this.values = values;
    }

    private Params(HashMap<String,Object> namedValues) {
        this.namedValues = namedValues;
    }

    // @Override
    public String toString() {
        //return Arrays.toString(values);
        return toJson();
    }


    public int size(){
        return (values != null? values.length : namedValues.size());
    }

    public Set<String> keys() {return (namedValues==null? null : namedValues.keySet());}

    public <T> T getValue(String key) {return (T) (namedValues==null? null : namedValues.get(key));}

    public <T> T get(int index){
        return (T) (values==null? null : values[index]);
    }

    public <T> T getFirst(){
        return get(0);
    }

    public <T> T getSecond(){
        return get(1);
    }

    public boolean isNone(){
        return this == None;
    }

    public static Params of(Object... values){
        return values.length == 0 ? Params.None : new Params(values);
    }

    public static Params ofPairs(Object... values){

        if (values.length % 2 != 0) {
            throw new RuntimeException("Params.ofPairs() takes even number of arguments");
        }
        // if pairs.length==0, it returns an empty pair Params
        // e.g. Params.ofPairs() returns an empty pair Params
        HashMap<String,Object> pairs = new HashMap<String,Object>();
        for (int i=0; i<values.length; i += 2) {
            pairs.put((String) values[i], values[i + 1]);
        }
        return new Params(pairs);
    }

    public final void addParam(String key, Object value) {namedValues.put(key,value);}

    public static Params deserialize(String json){
        return gson().fromJson(json, Params.class);
    }

    public String toJson(){
        return gson().toJson(this);
    }

    public List asList() {
        return Arrays.<Object>asList(values);
    }

    public <T> T getThird() {
        return get(2);
    }

    public Params remainingFrom(int index){
        return (values==null? null : of(Arrays.copyOfRange(values, index, values.length)));
    }

    public final boolean isPairs() {
        return (namedValues!=null);
    }

    public static class ParamsSerializer implements JsonSerializer<Params>, JsonDeserializer<Params>{

        @Override
        public JsonElement serialize(Params params, Type type, JsonSerializationContext context) {
            if (params.isPairs()) {
                final JsonObject ret = new JsonObject();
                for (String key : params.keys()) {
                    ret.add(key,GsonUtil.serializeWithType(params.getValue(key), context));
                }
                return ret;
            }
            else {
                final JsonArray ret = new JsonArray();
                for (Object each : params.values) {
                    ret.add(GsonUtil.serializeWithType(each, context));
                }
                return ret;
            }
        }

        @Override
        public Params deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context) throws JsonParseException {
            if (jsonElement instanceof JsonArray) {
                JsonArray array = (JsonArray) jsonElement;
                Object[] arr = new Object[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    JsonObject each = (JsonObject) array.get(i);
                    arr[i] = GsonUtil.deserializeWithType(each, context);
                }
                return Params.of(arr);
            }
            else {
                JsonObject jsonObj = jsonElement.getAsJsonObject();
                Params params = Params.ofPairs();
                for (Map.Entry<String,JsonElement> entry: jsonObj.entrySet()) {
                    params.addParam(entry.getKey(),GsonUtil.deserializeWithType(entry.getValue(),context));
                }
                return params;
            }
        }
    }

    public static final void main(String[] args) {
        Params params = Params.ofPairs();
        System.out.println(params.isPairs());
    }
}
