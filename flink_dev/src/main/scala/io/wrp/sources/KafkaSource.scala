package io.wrp.sources

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * @author DELL
 * @date 2021/2/26 16:41
 * @description 集成Kafka，作为 Flink 的source
 */
object KafkaSource {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.api.scala._

    // 开启checkpoint，及其配置
    env.enableCheckpointing(100)
    // 设置模式为exactly-once （这是默认值）
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 确保检查点之间有至少500 ms的间隔【checkpoint最小间隔】
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 检查点必须在一分钟内完成，或者被丢弃【checkpoint的超时时间】
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 同一时间只允许进行一个检查点
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    /**
     * ExternalizedCheckpointCleanup 配置项定义了当作业取消时，对作业 checkpoint 的操作：
     *
     * ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION：
     *      当作业取消时，保留作业的 checkpoint。注意，这种情况下，需要手动清除该作业保留的 checkpoint。
     * ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION：
     *      当作业取消时，删除作业的 checkpoint。仅当作业失败时，作业的 checkpoint 才会被保留。
     */
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)


    /**
     * kafka consumer
     */
    val properties = new Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092,hadoop02:9092,hadoop03:9092")
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink_consumer1")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")

    var kafkaSource = new FlinkKafkaConsumer[String](
      "test",
      new SimpleStringSchema, properties)
    kafkaSource.setCommitOffsetsOnCheckpoints(true)
    //设置statebackend
    env setStateBackend(
      new MemoryStateBackend(true))

    val stream: DataStream[String] = env.addSource(kafkaSource)
    stream.print()
    env.execute()
  }
}
