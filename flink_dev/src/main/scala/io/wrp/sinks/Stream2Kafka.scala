package io.wrp.sinks

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

/**
  * @author LYleonard
  * @date 2021/2/25 10:16
  * @description TODO
  */
object Stream2Kafka {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val stream: DataStream[String] = env.fromElements("Flink 测试")
    stream.print().setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "master:9092,slave1:9092,slave2:9092")

    val producer = new FlinkKafkaProducer("test", new SimpleStringSchema(), properties)
    stream.addSink(producer)

    env.execute()
  }

}
