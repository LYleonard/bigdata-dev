//package streaming.kafka
//
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
///**
//  * @Author LYleonard
//  * @Date 2020/6/2 15:27
//  * @Description 	整合kafka，sparkStreaming使用kafka 0.8API基于recevier来接受消息.
//  *             spark-streaming-kafka-0-8版本，Receiver-based Approach-不推荐使用!
//  *             此方法可能会在失败时丢失数据，虽然可以通过启动SparkStreaming WAL确保零数据丢失，
//  *             以便在发生故障时可以恢复所有数据，但是性能不好。
//  */
//object KafkaReceiver8 {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org").setLevel(Level.ERROR)
//    //1. 创建StreamingContext对象
//    val sparkConf = new SparkConf().setAppName("KafkaReceiver8").setMaster("local[*]")
//      .set("spark.streaming.receiver.writeAheadLog.enable", "true") //开启预写日志，存入HDFS
//    val ssc = new StreamingContext(sparkConf, Seconds(4))
//    ssc.checkpoint("hdfs://master:8020/spark/streaming/wal")
//
//    //2. 接收kafka数据
//    val zkQuorum = "master:2181,slave1:2181,slave2:2181"
//    val groupId = "KafkaReceiver8"
//    val topics = Map("test" -> 1)
//
//    //（String, String）元组的第一个元素是消息的key， 第二个元素是消息的value
//    val receiverDStreaam = KafkaUtils.createStream(ssc, zkQuorum, groupId, topics)
//
//    //获取topic的数据
//     val data = receiverDStreaam.map(_._2)
//
//    //单词统计
//    val result = data.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)
//
//    result.print()
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
