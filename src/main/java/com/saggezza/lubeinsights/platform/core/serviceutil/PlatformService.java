package com.saggezza.lubeinsights.platform.core.serviceutil;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URLDecoder;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.saggezza.lubeinsights.platform.core.common.metadata.ZKUtil;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 * Created by chiyao on 7/17/14.
 */

/**
 * common behaviors of platform services
 * A platform service act as an http server as well as http client.
 * Request can be synchronous as well as asynchronous
 * Any subclass only need to implement processRequest()
 *
 * */
public abstract class PlatformService {

    public static final Logger logger = Logger.getLogger(PlatformService.class);
    public static final int PLATFORM_SERVICE_PORT = 8080;
    public static final String Default = "default";
    public static final String Command = "command";
    protected ServiceName name;
    protected int port;
    protected String address;
    protected AbstractHandler handler;
    protected ServiceGateway serviceGateway = new ServiceGateway(); // client interface
    protected Server jettyServer;

    public PlatformService(ServiceName name) {
        this.name = name;
    }

    public final void start() throws IOException, InterruptedException, Exception {
        this.start(PLATFORM_SERVICE_PORT);
    }

    /**
     * start the service at this port
     * @param port
     * @throws IOException
     * @throws InterruptedException
     * @throws Exception
     */
    public final void start(int port) throws IOException, InterruptedException, Exception {

        this.port = port;
        // start the jetty server
        if (jettyServer == null) {
            jettyServer = new Server(port);
        }
        else {
            throw new Exception("Service " + name  + " already started");
        }
        Server jettyServer = new Server(port);
        handler = new AbstractHandler() {
            @Override
             public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
                try {
                    // translate to service language
                    ServiceRequest serviceRequest = HttpServletRequest2ServiceRequest(request);
                    String command = request.getParameter(Command);
                    command = command == null ? Default : command;
                    ServiceResponse serviceResponse = processRequest(serviceRequest, command);
                    response.setContentType("text/html;charset=utf-8");
                    response.setStatus(HttpServletResponse.SC_OK);
                    baseRequest.setHandled(true);
                    response.getWriter().println(serviceResponse.toJson()); // just pass on the json representation as the returned content
                }catch (IOException | RuntimeException e){
                    logger.error(e);
                    throw e;
                }
            }
        };

        jettyServer.setHandler(handler);
        // TODO: can extend to context or servlet based handlers
        jettyServer.start();
        address = genAddress(InetAddress.getLocalHost().getCanonicalHostName(), port);
        // TODO: need a complete way to get host name
        // register to service catalog
        ServiceCatalog.serviceOn(name, address);
    }

    private static final String genAddress(String host, int port) {
        return new StringBuilder().append(host).append(":").append(port).toString();
    }

    /**
     * translate to service language
     * @param request
     * @return
     */
    private ServiceRequest HttpServletRequest2ServiceRequest(HttpServletRequest request) {

        try {
            String requestLoad = request.getParameter("request");
            String queryDecoded = URLDecoder.decode(requestLoad, "UTF-8");
            System.out.println(queryDecoded);

            ServiceRequest serviceRequest = ServiceRequest.fromJson(queryDecoded);  // query is: "resuest=***"
            System.out.println("serviceRequest");
            System.out.println(serviceRequest.toJson());
            return serviceRequest;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void stop() throws Exception {
        System.out.println("Stopping service " + name);
        logger.warn("Stopping service " + name);
        if (jettyServer != null) {
            jettyServer.stop();
        }
        System.out.println("jetty server done");
        ServiceCatalog.serviceOff(this.name);
        ZKUtil.close();
    }


    /**
     * synchronous call to another service
     * @param serviceName
     * @param request
     * @return
     */
    public ServiceResponse sendRequest(ServiceName serviceName, ServiceRequest request)
        throws InterruptedException, TimeoutException, ExecutionException, Exception {

        return serviceGateway.sendRequest(serviceName, request);
    }

    /**
     * asynchronous call to another service
     * @param serviceName
     * @param request
     * @return
     */
    public void sendRequestAsync(ServiceName serviceName, ServiceRequest request, ServiceResponseHandler handler) {

        serviceGateway.sendRequestAsync(serviceName, request, handler);
    }

    /**
     * process a request. This is to be implemented by subclasses
     * @param request
     * @param command
     * @return
     */
    public abstract ServiceResponse processRequest(ServiceRequest request, String command);


}
