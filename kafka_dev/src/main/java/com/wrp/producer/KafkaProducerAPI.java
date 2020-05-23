package com.wrp.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @ClassName KafkaProducer
 * @Author LYleonard
 * @Date 2020/5/23 22:12
 * @Description Kafka producer API
 * Version 1.0
 **/
public class KafkaProducerAPI {
    public static void main(String[] args) {
        Properties props = new Properties();
        //kafka cluster
        props.put("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");

        /**
         * acks代表消息确认机制： 1 0 -1 all
         * acks = 0：表示producer请求立即返回，不需要等待leader的任何确认，该方案具有最高的
         *           吞吐率，但不保证消息是否真正发送成功
         *
         * acks = 1：表示leader分区必须应答此producer请求并写入消息到本地日志，之后producer
         *           请求被确认成功，如果leader挂掉有数据丢失风险
         *
         * acks = -1 或 all：表示分区leader必须等待消息被写入到所有的IRS副本中（同步副本）
         *           才任务producer请求成功，该方案提供了最高的消息持久性保证，但理论上
         *           吞吐率也是最低的
         */
        props.put("acks", "1");

        //重试次数
        props.put("retries", 0);

        //缓冲区大小 默认为32M
        props.put("buffer.memory", 33554432);

        //批处理数据大小，每次写入多少数据到topic ，默认为16KB
        props.put("batch.size", 16384);

        // 可延长多久发送数据， 默认为0：表示不等待立即发送
        props.put("linger.ms", 1);

        // 指定key、value的序列化器
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        for (int i = 0; i < 100; i++) {
            // 需要3个参数，第一个参数：topic名称， 第二个参数：表示消息的key， 第三个参数：消息内容
            producer.send(new ProducerRecord<String, String>("test",
                    Integer.toString(i), "Hello-Kafka-message-" + i));
        }

        producer.close();
    }
}
