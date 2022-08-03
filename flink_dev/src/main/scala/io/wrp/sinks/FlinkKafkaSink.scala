package io.wrp.sinks

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

/**
  * @author LYleonard
  * @date 2021/11/19 23:17
  * @description kafka作为flink的sink使用
  */
object FlinkKafkaSink {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //开启Checkpoint
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointInterval(500)
    env.getCheckpointConfig.setCheckpointTimeout(6000)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

    //设置stateBackend
    env.setStateBackend(new MemoryStateBackend())

    val text = env.socketTextStream("master", 9000)
    val topic = "test"
    val prop = new Properties()
    prop.setProperty("bootstrap.servers", "master:9092")
    prop.setProperty("transaction.timeout.ms", 60000*15+"")

    val myProducer = new FlinkKafkaProducer011[String](topic,
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()), prop,
      FlinkKafkaProducer011.Semantic.EXACTLY_ONCE)

    text.addSink(myProducer)

    env.execute("StreamingFromCollectionScala")
  }
}
