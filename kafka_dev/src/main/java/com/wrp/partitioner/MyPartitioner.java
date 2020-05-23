package com.wrp.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @ClassName MyPartitioner
 * @Author LYleonard
 * @Date 2020/5/24 0:31
 * @Description 自定义kafka的分区函数
 * Version 1.0
 **/
public class MyPartitioner implements Partitioner {
    /**
     * Compute the partition for the given record.
     * 通过这个方法来实现消息要去哪一个分区中
     *
     * @param topic      The topic name
     * @param key        The key to partition on (or null if no key)
     * @param keyBytes   The serialized key to partition on( or null if no key)
     * @param value      The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param cluster    The current cluster metadata
     */
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 获取topic分区数
        int partitons = cluster.partitionCountForTopic(topic);
        //key.hashCode()可能会出现负数 -1 -2 0 1 2
        //Math.abs 取绝对值
        return Math.abs(key.hashCode() % partitons);
    }

    /**
     * This is called when partitioner is closed.
     */
    public void close() {
    }

    /**
     * Configure this class with the given key-value pairs
     *
     * @param configs
     */
    public void configure(Map<String, ?> configs) {
    }
}
