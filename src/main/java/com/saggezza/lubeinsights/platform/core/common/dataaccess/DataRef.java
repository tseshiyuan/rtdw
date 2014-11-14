package com.saggezza.lubeinsights.platform.core.common.dataaccess;

import com.google.gson.*;
import com.saggezza.lubeinsights.platform.core.common.GsonUtil;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;

import java.lang.reflect.Type;

/**
 * Created by chiyao on 7/28/14.
 */
public class DataRef {

    private DataRefType type;
    private Object val;
    private DataModel dataModel;//Optional dataref

    public DataRef(DataRefType type, Object val) {
        this.type = type;
        this.val = val;
    }

    public DataRef(DataModel dataModel) {
        this.dataModel = dataModel;
        this.type = DataRefType.MODEL;
    }

    public DataRef(DataRefType type, Object val, DataModel dataModel) {
        this.type = type;
        this.val = val;
        this.dataModel = dataModel;
    }

    public DataModel getDataModel() { return dataModel; }
    public DataRefType getType() {return type;}
    public String getFileName() {return null == val ? null : val.toString();}
    public <T> T getValue(){
        return (T)val;
    }

    public static class DataRefSerialization implements JsonSerializer<DataRef>, JsonDeserializer<DataRef> {

        @Override
        public DataRef deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
            JsonObject jsonObject = (JsonObject) jsonElement;
            String typeName = jsonObject.get("type").getAsString();
            Object val = GsonUtil.deserializeWithType(jsonObject.get("val"), jsonDeserializationContext);
            Object dataModel = GsonUtil.deserializeWithType(jsonObject.get("dataModel"), jsonDeserializationContext);
            return new DataRef(DataRefType.valueOf(typeName), val, (DataModel) dataModel);
        }

        @Override
        public JsonElement serialize(DataRef dataRef, Type type, JsonSerializationContext jsonSerializationContext) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("type", dataRef.getType().name());
            jsonObject.add("val", GsonUtil.serializeWithType(dataRef.val, jsonSerializationContext));
            jsonObject.add("dataModel", GsonUtil.serializeWithType(dataRef.dataModel, jsonSerializationContext));
            return jsonObject;
        }
    }

}
