package streaming.kafka

import org.apache.kafka
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author LYleonard
  * @Date 2020/6/3 15:52
  * @Description sparkStreaming使用spark-streaming-kafka-0-10 API基于Direct直连来接受消息
  */
object KafkaDirect10 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("SparkStreamingDirectKafka08").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(6))

    //使用direct方式接收kafka数据
    val topic = Set("test")
    val kafkaParas = Map(
      "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
      "group.id" -> "KafkaDirect10",
      "key.deserializer" -> classOf[StringDeserializer]
    )
  }
}
