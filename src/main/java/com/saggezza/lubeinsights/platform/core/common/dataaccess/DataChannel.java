package com.saggezza.lubeinsights.platform.core.common.dataaccess;

import com.google.gson.Gson;
import com.saggezza.lubeinsights.platform.core.common.GsonUtil;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by chiyao on 7/28/14.
 */

/**
 * A DataChannel is a map from identifier to DataRef (A DataRef is a value, file, or table.)
 * A DataChannel is passed around among workflow nodes for them to refer any DataRef by names (identifiers) and process
 */
public class DataChannel {

    ConcurrentHashMap<String,DataRef> channel = new ConcurrentHashMap<String,DataRef>();

    public DataChannel() {}

    private DataChannel(ConcurrentHashMap<String,DataRef> channel) {
        this.channel = channel;
    }

    /**
     * merge a dataChannel into this
     * @param dataChannel
     * @return the merged dataChannel
     */
    public final void merge(DataChannel dataChannel) {
        if (dataChannel != null) {
            channel.putAll(dataChannel.channel);
        }
    }

    public final Set<String> getTags() {
        return channel.keySet();
    }

    public final boolean isEmpty() {return channel.isEmpty();}

    /**
     * put the DataRef into the DataChannel under the tag
     * @param tag
     * @param dataRef
     */
    public final void putDataRef(String tag, DataRef dataRef) {
        channel.put(tag,dataRef);
    }

    /**
     * get the DataRef for this tag in this DataChannel
     * @param tag
     * @return the DataRef for this tag
     */
    public final DataRef getDataRef(String tag) {
        return channel.get(tag);
    }

    public final DataRef getSingleDataRef(){
        if(channel.size() == 1){
            return channel.values().iterator().next();
        }
        return null;
    }

    /**
     * get the only DataRef in this DataChannel
     * @return the sole DataRef in this DataChannel
     */
    public final DataRef getDataRef() throws DataChannelException {
        if (channel.size()>1) {
            throw new DataChannelException("getDataRef found " + channel.size() + " DataRef in DataChannel");
        }
        for (String tag:channel.keySet()) {
            return channel.get(tag);
        }
        return null;
    }

    public final String toJson() {
        return GsonUtil.gson().toJson(this);
    }

    public static final DataChannel fromJson(String json) {
        return GsonUtil.gson().fromJson(json, DataChannel.class);
    }
}
