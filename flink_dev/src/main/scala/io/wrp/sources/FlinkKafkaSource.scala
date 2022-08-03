package io.wrp.sources

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * @author LYleonard
  * @date 2021/11/18 21:34
  * @description kafka作为flink的source
  */
object FlinkKafkaSource {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    //隐式转换
    import org.apache.flink.api.scala._

    //checkpoint配置
    environment.enableCheckpointing(100)

    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    environment.getCheckpointConfig.setCheckpointTimeout(60000)
    environment.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    environment.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

    val topic = "test"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092")
    prop.setProperty("group.id", "flink-group")
    prop.put("auto.offset.reset", "earliest")
    prop.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    prop.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")

    val kafkaSource: FlinkKafkaConsumer011[String] =
      new FlinkKafkaConsumer011[String](topic, new SimpleStringSchema(), prop)

    //指定消费者是否应该在检查点上向Kafka提交偏移量。
    kafkaSource.setCommitOffsetsOnCheckpoints(true)

    //设置stateBackend
    environment.setStateBackend(new MemoryStateBackend())

    val result: DataStream[String] = environment.addSource(kafkaSource)
    result.print()

    environment.execute()
  }
}
