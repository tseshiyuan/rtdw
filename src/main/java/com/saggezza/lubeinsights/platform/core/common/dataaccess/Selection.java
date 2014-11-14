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
    private String[] aliases;

    public static final Selection Empty = new Selection();

    private Selection(){
        this.indices = new int[]{};
        this.names = null;
    }

    private Selection(int[] indices, String[] names, String[] aliases){
        this.indices = indices;
        this.names = names;
        this.aliases = aliases;
    }

    public Selection(int... indices) {
        Preconditions.checkNotNull(indices);
        Preconditions.checkArgument(indices.length > 0);
        this.indices = indices;
        this.names = null;
    }

    public Selection(int[] indices, String[] aliases) {
        Preconditions.checkNotNull(indices);
        Preconditions.checkArgument(indices.length > 0);
        Preconditions.checkArgument(indices.length == aliases.length);
        this.indices = indices;
        this.names = null;
        this.aliases = aliases;
    }

    public Selection(String... names) {
        Preconditions.checkNotNull(names);
        Preconditions.checkArgument(names.length > 0);
        this.names = names;
    }

    public String[] getAliases() {
        return aliases;
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

    public boolean isAliasBased(){
        return aliases != null;
    }

    public int[] getIndices() {
        return Arrays.copyOf(indices, indices.length);
    }

    public int length(){
        return isIndexBased() ? indices.length : names.length;
    }

    public DataElement from(DataElement element){
        if(isNamedBased()){
            LinkedHashMap<String, DataElement> map = new LinkedHashMap<>();
            for(String name : names){
                 map.put(name, element.valueByName(name));
            }
            return new DataElement(map);
        }else if(isAliasBased()){
            LinkedHashMap<String, DataElement> map = new LinkedHashMap<>();
            for(int i=0; i < indices.length; i++){
                map.put(aliases[i], element.valueAt(indices[i]));
            }
            return new DataElement(map);
        } else {
            ArrayList<DataElement> elems = new ArrayList<>();
            for(int eachIndex : indices){
                elems.add(element.valueAt(eachIndex));
            }
            return new DataElement(elems);
        }
    }

    public DataModel from(DataModel dataModel){//Select transforms into a list based data model since select is ordered
        if(isNamedBased()){
            LinkedHashMap<String, DataModel> map = new LinkedHashMap<>();
            for(String name : names){
                map.put(name, dataModel.getMap().get(name));
            }
            return new DataModel(map);
        }else if (isAliasBased()){
            LinkedHashMap<String, DataModel> map = new LinkedHashMap<>();
            for(int i=0; i < indices.length; i++){
                map.put(aliases[i], dataModel.getList().get(indices[i]));
            }
            return new DataModel(map);
        }else{
            ArrayList<DataModel> elems = new ArrayList<>();
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
            JsonElement intArray = jsonObject.get("_ind");
            JsonElement aliArray = jsonObject.get("_ali");
            JsonElement namArray = jsonObject.get("_nam");
            int[] content = asIntArray(intArray);
            String[] alias = asStringArray(aliArray);
            String[] names = asStringArray(namArray);
            return new Selection(content, names, alias);
        }

        private String[] asStringArray(JsonElement namArray){
            if(namArray == null || namArray.isJsonNull()){
                return null;
            }
            JsonArray jsonArray = namArray.getAsJsonArray();
            String[] ret = new String[jsonArray.size()];
            for(int i=0; i < ret.length; i++){
                ret[i] = jsonArray.get(i).getAsString();
            }
            return ret;
        }

        private int[] asIntArray(JsonElement intArray) {
            if(intArray == null || intArray.isJsonNull()){
                return null;
            }
            JsonArray jsonArray = intArray.getAsJsonArray();
            int[] content = new int[jsonArray.size()];
            for(int i=0; i < content.length; i++){
                content[i] = jsonArray.get(i).getAsInt();
            }
            return content;
        }

        @Override
        public JsonElement serialize(Selection selection, Type type, JsonSerializationContext context) {
            JsonObject object = new JsonObject();
            object.addProperty("_type", Selection.class.getName());
            object.add("_ind", context.serialize(selection.indices));
            object.add("_nam", context.serialize(selection.names));
            object.add("_ali", context.serialize(selection.aliases));
            return object;
        }
    }

}
