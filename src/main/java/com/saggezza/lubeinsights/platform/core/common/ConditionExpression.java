package com.saggezza.lubeinsights.platform.core.common;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.saggezza.lubeinsights.platform.core.common.Params;

import java.io.Serializable;
import java.util.Arrays;

import static com.saggezza.lubeinsights.platform.core.common.GsonUtil.gson;

/**
 * @author : Albin
 */
public class ConditionExpression implements Serializable{

    private String predicate;
    private Params params;

    public ConditionExpression(String predicate,
                               Params params) {
        this.predicate = predicate;
        this.params = params;
    }

    public String getPredicate() {
        return predicate;
    }

    public Params getParams(){
        return params;
    }

    public String toJson(){
        return gson().toJson(this);
    }

    public static ConditionExpression deserialize(String json){
        return gson().fromJson(json, ConditionExpression.class);
    }

}
