package com.saggezza.lubeinsights.platform.core.common;

import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @author : Albin
 */
public class Utils {

    public static final DateFormat CurrentDateSecond = new SimpleDateFormat("dd MMM yyyy HH:mm:ss");

    private Utils(){}

    public static String currentTime(){
        return CurrentDateSecond.format(Calendar.getInstance().getTime());
    }

    public static String hostWithPort(URI uri){
        return uri.getHost()+ (uri.getPort() != -1 ? (":"+ uri.getPort()) : "");
    }

    public static String serverId(URI uri){
        return uri.getScheme()+"://"+ hostWithPort(uri);
    }

    public static String path(URI uri){
        return uri.toString().replace(serverId(uri), "");
    }

}
