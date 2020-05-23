package com.wrp.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @ClassName KafkaConsumerControllerOffset
 * @Author LYleonard
 * @Date 2020/5/23 23:36
 * @Description kafka消费者（手动提交偏移量）
 * Version 1.0
 **/
public class KafkaConsumerControllerOffset {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //kafka cluster
        properties.put("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");

        //消费者组id
        properties.put("group.id", "consumerControllerOffset");
        //手自动提交偏移量
        properties.put("enable.auto.commit", "false");

        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("test"));

        //定义一个数字，表示消息达到多少后手动提交偏移量
        final int minBatchSize = 20;

        //定义一个数组，缓冲一批数据
        List<ConsumerRecord<String, String>> buffer = new ArrayList<ConsumerRecord<String, String>>();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                System.out.println("缓存区的数据条数: " + buffer.size());
                consumer.commitSync();
                buffer.clear();
                System.out.println("已处理该批数据...");
            }
        }
    }
}
