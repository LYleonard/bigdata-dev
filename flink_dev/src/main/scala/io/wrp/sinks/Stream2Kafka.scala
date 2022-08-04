package io.wrp.sinks

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

/**
  * @author LYleonard
  * @date 2021/2/25 10:16
  * @description kafka 作为 Flink 的sink使用
  */
object Stream2Kafka {
  def main(args: Array[String]): Unit = {
//
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    import org.apache.flink.api.scala._
//    val stream: DataStream[String] = env.fromElements("Flink 测试")
//    stream.print().setParallelism(1)
//
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
//
//    val producer = new FlinkKafkaProducer("test", new SimpleStringSchema(), properties)
//    stream.addSink(producer)
//
//    env.execute()

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //启用checkpoint,及配置
    env.enableCheckpointing(5000)
    // 设置模式为exactly-once
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 同一时间只允许进行一个检查点
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 设置ExternalizedCheckpointCleanup 配置项，定义了当作业取消时，对作业 checkpoint 的操作
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.setStateBackend(new MemoryStateBackend(true))


    val stream: DataStream[String] = env.fromElements("Flink 测试")
    stream.print().setParallelism(1)

    val properties = new Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      "hadoop01:9092,hadoop02:9092,hadoop03:9092")
    //第一种解决方案，设置FlinkKafkaProducer的事务超时时间
    //设置事务超时时间
    properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 60000*15+"");
    //第二种解决方案，在kafka 配置中设置最大事务超时时间

    val producer = new FlinkKafkaProducer("test",
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
      properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)

    stream.addSink(producer)

    env.execute()
  }

}
