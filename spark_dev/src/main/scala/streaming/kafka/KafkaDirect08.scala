//package streaming.kafka
//
//import kafka.serializer.StringDecoder
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.dstream.InputDStream
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
//  * @Author LYleonard
//  * @Date 2020/6/3 14:11
//  * @Description sparkStreaming使用kafka 0.8API基于Direct直连来接受消息
//  *              spark direct API接收kafka消息，不需要经过zookeeper，直接从broker上获取信息。
//  */
//object KafkaDirect08 {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    val sparkConf = new SparkConf().setAppName("SparkStreamingDirectKafka08").setMaster("local[*]")
//    val ssc = new StreamingContext(sparkConf, Seconds(4))
//
//    //接收kafka数据
//    val kafkaParas = Map(
//      "metadata.broker.list" -> "master:9092,slave1:9092,slave2:9092",
//      "group.id" -> "kafkaDirect08"
//    )
//    val topics = Set("test")
//
//    //使用Direct方式接收数据
//    val kafkaDStream: InputDStream[(String, String)] =
//      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParas,topics)
//
//    //获取topic数据
//    val data = kafkaDStream.map(_._2)
//    val result = data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
//    result.print()
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
