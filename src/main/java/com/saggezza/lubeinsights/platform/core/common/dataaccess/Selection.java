package com.saggezza.lubeinsights.platform.core.common.dataaccess;

import com.google.common.base.Preconditions;
import com.google.gson.*;
import com.saggezza.lubeinsights.platform.core.common.datamodel.DataModel;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.*;

/**
 * @author : Albin
 */
public final class Selection implements Serializable{

    private int[] indices;//TODO - make fields final. See if it causes trouble with Gson.
    private String[] names;

    private Selection(){
    }

    public Selection(int... indices) {
        Preconditions.checkNotNull(indices);
        Preconditions.checkArgument(indices.length > 0);
        this.indices = indices;
        this.names = null;
    }

    public Selection(String... names) {
        Preconditions.checkNotNull(names);
        Preconditions.checkArgument(names.length > 0);
        this.names = names;
    }

    public String[] getNames(){
        return names;
    }

    public boolean isIndexBased(){
        return indices != null;
    }

    public boolean isNamedBased(){
        return names != null;
    }

    public int[] getIndices() {
        return Arrays.copyOf(indices, indices.length);
    }

    public int length(){
        return isIndexBased() ? indices.length : names.length;
    }

    public DataElement from(DataElement element){
        ArrayList<DataElement> elems = new ArrayList<>();
        if(isNamedBased()){
            for(String name : names){
                elems.add(element.valueByName(name));
            }
            return new DataElement(elems);
        }else {
            for(int eachIndex : indices){
                elems.add(element.valueAt(eachIndex));
            }
            return new DataElement(elems);
        }
    }

    public DataModel from(DataModel dataModel){//Select transforms into a list based data model since select is ordered
        ArrayList<DataModel> elems = new ArrayList<>();
        if(isNamedBased()){
            for(String name : names){
                elems.add(dataModel.getMap().get(name));
            }
            return new DataModel(elems);
        }else{
            for(int eachIndex : indices){
                elems.add(dataModel.getList().get(eachIndex));
            }
            return new DataModel(elems);
        }
    }

    public int getAt(int index){
        if(!isIndexBased()){
            throw new RuntimeException("Not index based");
        }
        return indices[index];
    }

    public String getNameAt(int index){
        if(!isNamedBased()){
            throw new RuntimeException("Not name based");
        }
        return names[index];
    }


    public static Selection Range(int start, int end){
        if(end <= start){
            throw new RuntimeException("Not a valid combination for start and end "+start+" - "+end);
        }
        int[] selection = new int[end - start];
        for(int i=start, index = 0; i < end; i++, index++){
            selection[index] = i;
        }
        return new Selection(selection);
    }

    public static class SelectionSerializer implements JsonSerializer<Selection>, JsonDeserializer<Selection>{

        @Override
        public Selection deserialize(JsonElement jsonElement, Type type, JsonDeserializationContext context) throws JsonParseException {
            JsonObject jsonObject = (JsonObject) jsonElement;
            JsonElement element = jsonObject.get("_ser");
            JsonArray jsonArray = element.getAsJsonArray();
            int[] content = new int[jsonArray.size()];
            for(int i=0; i < content.length; i++){
                content[i] = jsonArray.get(i).getAsInt();
            }
            return new Selection(content);
        }

        @Override
        public JsonElement serialize(Selection selection, Type type, JsonSerializationContext context) {
            JsonObject object = new JsonObject();
            object.addProperty("_type", Selection.class.getName());
            object.add("_ser", context.serialize(selection.getIndices()));
            return object;
        }
    }

}
