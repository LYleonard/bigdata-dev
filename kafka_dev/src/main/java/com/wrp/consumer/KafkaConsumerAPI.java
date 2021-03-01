package com.wrp.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @ClassName KafkaConsumerAPI
 * @Author LYleonard
 * @Date 2020/5/23 22:13
 * @Description kafka consumer API
 * Version 1.0
 **/
public class KafkaConsumerAPI {
    public static void main(String[] args) {
        Properties properties = new Properties();
        //kafka cluster
        properties.put("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");

        //消费者组id
        properties.put("group.id", "consumer-test");
        //自动提交偏移量
        properties.put("enable.auto.commit", "true");
        // 自动提交偏移量的时间间隔
        properties.put("auto.commit.interval.ms", "1000");
        /**
         * 默认是latest
         * earliest：当各分区下有已提交的offset时，从提交的offset开始消费，无提交的offset时，从头开始消费
         *
         * latest：当各分区下有已提交的offset时，从提交的offset开始消费，无提交的offset时。消费该分区下新产生的数据
         *
         * none：topic各分区都存在已提交的offset时，从offset后开始消费；只要有一个分区不存在已提交的offset，则抛出异常
         */
        properties.put("auto.offset.reset", "earliest");

        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        //指定消费哪些topic
        consumer.subscribe(Arrays.asList("test"));

        while (true) {
            // 不断拉取数据
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                //该消息所在的分区
                int partition = record.partition();
                //消息对应的key
                String key = record.key();
                //消息对应的偏移量
                long offset = record.offset();
                //消息体
                String value = record.value();
                System.out.println("partition:" + partition + "\tkey:" +
                        key + "\toffset:" + offset + "\tvalue:" + value);
            }
        }
    }
}
