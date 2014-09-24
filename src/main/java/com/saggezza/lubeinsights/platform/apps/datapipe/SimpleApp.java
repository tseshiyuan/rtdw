package com.saggezza.lubeinsights.platform.apps.datapipe;

/**
 * Created by chiyao on 7/23/14.
 */

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.saggezza.lubeinsights.platform.core.serviceutil.*;
import com.saggezza.lubeinsights.platform.core.workflowengine.*;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;

/**
 * This is a simple application that runs a simple data pipe
 */
public class SimpleApp {

    public static final Logger logger = Logger.getLogger(SimpleApp.class);

    protected ServiceGateway serviceGateway = new ServiceGateway();

    // start all platform services
    // This can be done by other applications such as Admin Console
    public static void prepareServices() {
        // start Data Engine

        // start WorkFlow Engine
    }

    public void run() {
        try {

            // build a simple workflow
            // run this workflow (workflow can be run by name or inline spec)

        } catch (Exception e) {
            logger.trace("SimpleApp run() error", e);
        }
    }

    public static final void main(String[] args) {
        SimpleApp app = new SimpleApp();
        app.prepareServices();
        app.run();
    }


}
