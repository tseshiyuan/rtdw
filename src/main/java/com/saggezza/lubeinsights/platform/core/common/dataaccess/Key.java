package com.saggezza.lubeinsights.platform.core.common.dataaccess;

import com.google.gson.*;
import com.saggezza.lubeinsights.platform.core.common.GsonUtil;

import java.io.Serializable;
import java.lang.reflect.Type;

/**
 * @author : Albin
 */
public interface Key extends Serializable {

    Selection keySpec();

    public static class KeySerializer implements JsonSerializer<Key>, JsonDeserializer<Key> {

        @Override
        public Key deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context)
                throws JsonParseException {

            return (Key) GsonUtil.deserializeWithType(jsonElement, context);
        }

        @Override
        public JsonElement serialize(Key key, Type type, JsonSerializationContext context) {
            return GsonUtil.serializeWithType(key, context);
        }
    }



}
