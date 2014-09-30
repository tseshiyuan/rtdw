package com.saggezza.lubeinsights.platform.apps.datapipe;

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.saggezza.lubeinsights.platform.core.dataengine.DataEngine;
import com.saggezza.lubeinsights.platform.core.serviceutil.*;

import java.util.ArrayList;

/**
 * @author : Albin
 */
public class DataEngineApp {

    public static void main(String[] args) {
        DataEngine engine = new DataEngine();

        ArrayList<ServiceRequest.ServiceStep> steps = new ArrayList<>();
//        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.PARSE,
//                asList(new ParseConfig("/Users/albin/Desktop/sample.csv",","))));
//        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.Filter,
//                asList(new FilterConfig(0, "CK113"))));
//        steps.add(new ServiceRequest.ServiceStep(ServiceCommand.WRITE,
//                asList(new WriteConfig("/Users/albin/Desktop/filtered.csv", "|"))));

        ServiceRequest request = new ServiceRequest(steps);
        ServiceResponse serviceResponse = engine.processRequest(request, null);

        System.out.println(serviceResponse.getMessage());
    }

    private static ArrayList<String> asList(Object o){
        return Lists.newArrayList(new Gson().toJson(o));
    }

}
