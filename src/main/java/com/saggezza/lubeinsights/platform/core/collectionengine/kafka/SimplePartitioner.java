package com.saggezza.lubeinsights.platform.core.collectionengine.kafka;

/**
 * Created by chiyao on 9/8/14.
 */
import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class SimplePartitioner implements Partitioner {
    public SimplePartitioner (VerifiableProperties props) {}

    public int partition(Object key, int a_numPartitions) {
        return ((String)key).hashCode() % a_numPartitions;
    }
}
