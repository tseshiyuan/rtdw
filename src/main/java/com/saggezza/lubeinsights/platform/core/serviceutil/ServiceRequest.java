package com.saggezza.lubeinsights.platform.core.serviceutil;

/**
 * Created by chiyao on 7/16/14.
 */

import com.google.gson.Gson;
import com.saggezza.lubeinsights.platform.core.common.GsonUtil;
import com.saggezza.lubeinsights.platform.core.common.Params;
import java.util.ArrayList;

import static com.saggezza.lubeinsights.platform.core.common.GsonUtil.gson;

/**
 * This class defines the format of a service request
 * A service request is a sequence of statements (ServiceStep). Each statement is a command with its arguments
 */
public class ServiceRequest {

    protected ArrayList<ServiceStep> commandList = new ArrayList<ServiceStep>();

    public ServiceRequest(ServiceCommand command, Params arguments) {
        commandList.add(new ServiceStep(command, arguments));
    }

    public ArrayList<ServiceStep> getCommandList() {
        return commandList;
    }

    public ServiceRequest(ArrayList<ServiceStep> commandList) {
        this.commandList = commandList;
    }

    public ServiceRequest addServiceStep(ServiceCommand command, Params arguments) {
        commandList.add(new ServiceStep(command,arguments));
        return this;  // for chaining calls easily
    }

    /**
     * insert command in front of serviceRequest and return a new request
     * @param command
     * @param arguments
     * @param serviceRequest
     * @return new request with command in front and old request following
     */
    public static final ServiceRequest createServiceRequest(ServiceCommand command, Params arguments, ServiceRequest serviceRequest) {

        ServiceRequest result = new ServiceRequest(command, arguments);
        for (ServiceStep step: serviceRequest.commandList) {
            result.commandList.add(step);
        }
        return result;
    }

    public final String toJson() {
        return gson().toJson(this);
    }

    public static final ServiceRequest fromJson(String json) {
        return gson().fromJson(json,ServiceRequest.class);
    }


    public static class ServiceStep {

        protected ServiceCommand command;
        protected Params params; // name,value,name,value,...

        // build from ArrayList
        public ServiceStep(ServiceCommand command, Params params) {
            this.command = command;
            this.params = params;
        }

        public ServiceCommand getCommand() {
            return command;
        }

        public Params getParams() {
            return params;
        }
    }


}
