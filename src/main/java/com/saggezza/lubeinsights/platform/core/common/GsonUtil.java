package com.saggezza.lubeinsights.platform.core.common;

import com.google.gson.*;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataElement;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataRef;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.Selection;

/**
 * @author : Albin
 */
public class GsonUtil {

    private GsonUtil(){}


    private static final Gson GSON = new GsonBuilder().
            registerTypeAdapter(Params.class, new Params.ParamsSerializer()).
            registerTypeAdapter(DataRef.class, new DataRef.DataRefSerialization()).
            registerTypeAdapter(Selection.class, new Selection.SelectionSerializer()).
            create();

    private static final String classId = "_cls";
    private static final String nullId = "null";
    private static final String serialized = "_ser";

    private static JsonElement nullSerialized(){
        JsonObject obj = new JsonObject();
        obj.addProperty(classId, nullId);
        return obj;
    }

    public static Gson gson(){
        return GSON;
    }

    public static JsonElement serializeWithType(Object value, JsonSerializationContext context){
        if(value == null){
            return nullSerialized();
        }else {
            JsonObject obj = new JsonObject();
            obj.addProperty(classId, value.getClass().getName());
            obj.add(serialized, context.serialize(value));
            return obj;
        }
    }

    public static Object deserializeWithType(JsonElement elem, JsonDeserializationContext context){
        JsonObject obj = (JsonObject) elem;
        String cls = obj.get(classId).getAsString();
        if(cls.equals(nullId)){
            return null;
        }else {
            try {
                Class vClass = Class.forName(cls);
                return context.deserialize(obj.get(serialized), vClass);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Class not found while deserializing", e);//TODO -fix exception
            }
        }
    }
}
