package com.saggezza.lubeinsights.platform.core.serviceutil;

import org.eclipse.jetty.client.ContentExchange;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.io.BufferCache;
import org.eclipse.jetty.io.ByteArrayBuffer;

import java.net.URI;
import java.net.URLEncoder;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Created by chiyao on 7/23/14.
 */
public class ServiceGateway {

    protected static ServiceGateway gateway = null;

    /**
     *
     * @return the singleton
     */
    public static final ServiceGateway getServiceGateway() {
        if (gateway==null) {
            gateway = new ServiceGateway();
        }
        return gateway;
    }


    /**
     * synchronous call to another service
     * @param serviceName
     * @param request
     * @return
     */
    public ServiceResponse sendRequest(ServiceName serviceName, ServiceRequest request)
            throws InterruptedException, TimeoutException, ExecutionException, Exception {
        return sendRequest(serviceName, request, null);
    }


    /**
     * synchronous call to another service
     * @param serviceName
     * @param request
     * @return
     */
    public ServiceResponse sendRequest(ServiceName serviceName, ServiceRequest request, String command)
            throws Exception {

        String address = ServiceCatalog.findAddress(serviceName);
        if (address==null) {
            throw new RuntimeException("Cannot find service "+serviceName+" to send requests to");
        }
        String[] split = address.split(":");
        String host = split[0];
        int port =  Integer.parseInt(split[1]);
        //System.out.println("request sent to: "+ address);
        //System.out.println(request.toJson());
        HttpClient httpClient = new HttpClient();
        try {
            httpClient.start();
            //URI uri = new URI("http", address, "request?", request.toJson());
            //URI uri = new URI("http", address, "/request?", "test");
            //URI uri = new URI("http", null, "192.168.1.79", 8081, "/", request.toJson(), null);
//            URI uri = new URI("http", null, "192.168.1.79", 8081, "/","request="+request.toJson(), null);
            URI uri = new URI("http", null, host, port, "/","", null);

            ContentExchange exchange = new ContentExchange(false);
            exchange.setURI(uri);
            exchange.setRequestContentType("application/x-www-form-urlencoded;charset=utf-8");
            exchange.setMethod("POST");
            String content = (command == null ? "" : "command="+command+"&") +
                    ("request=" + URLEncoder.encode(request.toJson(), "UTF-8"));
            exchange.setRequestContent(new ByteArrayBuffer(content));
            httpClient.send(exchange);
            int status = exchange.waitForDone();
            String responseContent = exchange.getResponseContent();

            return ServiceResponse.fromContentResponse(responseContent);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            if (httpClient != null) {
                httpClient.stop();
            }
        }
    }
/*
    public ServiceResponse sendRequest(ServiceName serviceName, ServiceRequest request) {

            try {
                if (httpClient == null) {
                    httpClient = new HttpClient();
                }
                String address = ServiceCatalog.findAddress(serviceName);
                String url = new StringBuilder(address).append("/request?").append(request.toJson()).toString();
                ContentExchange exchange = new ContentExchange(false);
                exchange.setURL(url);
                httpClient.send(exchange);
                int status = exchange.waitForDone();
                String responseContent = exchange.getResponseContent();
                return ServiceResponse.fromContentResponse(responseContent);
            }catch (Exception e){
                throw new RuntimeException(e);//TODO - fix exception
            }
        }
*/


        /**
         * asynchronous call to another service
         * @param serviceName
         * @param request
         * @return
         */
    public void sendRequestAsync(ServiceName serviceName, ServiceRequest request, ServiceResponseHandler handler) throws Exception {

        HttpClient httpClient = new HttpClient();
        try {
            httpClient.start();
            String address = ServiceCatalog.findAddress(serviceName);
            String url = new StringBuilder(address).append("/request?").append(request.toJson()).toString();
            ContentExchange exchange = new ContentExchange(false);
            exchange.setURL(url);
            httpClient.send(exchange);
            int result = exchange.waitForDone();
            handler.handle(ServiceResponse.fromResult(result));
            // TODO: there are other content handling in jetty
        }catch (Exception e){
            throw new RuntimeException(e);
        } finally {
            if (httpClient != null) {
                httpClient.stop();
            }
        }
    }

}
