package streaming.kafka

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
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
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "enable.auto.commit" -> "false"
    )
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topic, kafkaParas)
    )

    //对数据进行处理
    kafkaDStream.foreachRDD(rdd => {
      val dataRDD = rdd.map(_.value())
      dataRDD.foreach(line => {
        println(line)
      })

      //提交偏移量信息，把偏移量信息添加到kafka
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
