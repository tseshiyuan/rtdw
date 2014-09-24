package com.saggezza.lubeinsights.platform.core.serviceutil;

/**
 * Created by chiyao on 7/16/14.
 */

import com.google.gson.Gson;
import com.saggezza.lubeinsights.platform.core.common.GsonUtil;
import com.saggezza.lubeinsights.platform.core.common.dataaccess.DataChannel;

import static com.saggezza.lubeinsights.platform.core.common.GsonUtil.gson;

/**
 * This class defines the format of a service response
 */
public class ServiceResponse {

    private String status; // TBD
    private String message;
    private DataChannel data;
    private int errorCode;

    public ServiceResponse(String status, String message, DataChannel data) {
        this.status = status;
        this.message = message;
        this.data = data;
    }

    public ServiceResponse(int errorCode, String message) {
        this.errorCode = errorCode;
        this.message = message;
    }

    public final String toJson() {
        return GsonUtil.gson().toJson(this,ServiceResponse.class);
    }

    public static final ServiceResponse fromJson(String json) {
        return gson().fromJson(json, ServiceResponse.class);
    }

    public final DataChannel getData() {return data;}

    public final String getStatus() {return status;}

    public final String getMessage() {return message;}


    public static final ServiceResponse fromContentResponse(String contentResponse) {
        return ServiceResponse.fromJson(contentResponse);
    }

    public static final ServiceResponse fromResult(int result) {
        // TODO
        return null;
    }

    public boolean isError(){
        return errorCode != 0;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
