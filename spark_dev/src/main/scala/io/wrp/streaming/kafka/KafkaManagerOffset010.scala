package streaming.kafka

import kafka.utils.{ZKGroupTopicDirs,ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author LYleonard
  * @Date 2020/6/5 16:06
  * @Description 使用直连方式 SparkStreaming连接kafka0.10获取数据
  *             手动将偏移量数据保存到zookeeper中
  */
object KafkaManagerOffset010 {
  def main(args: Array[String]): Unit = {

    //todo:1、构建StreamingContext对象
    val sparkConf = new SparkConf().setAppName("KafkaManagerOffset010").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    //消费者ID
    val groupId = "consumer-010"

    //配置消费者参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "master:9092,slave1:9092,slave2:9092",
      "group.id" -> groupId,
      //从头的数据开始消费earliest
      "auto.offset.reset" -> "earliest",
      //是否自动提交偏移量
      "enable.auto.commit" -> "false"
    )

    val topic = "helloTopic"
    val topics = Array(topic)

    val zkTopicDirs: ZKGroupTopicDirs = new ZKGroupTopicDirs(groupId, topic)

    //ZK储存offset的目录
    val offsetDir: String = zkTopicDirs.consumerOffsetDir

    //创建一个zkClient的客户端连接
    val zkQuorum = "master:2181,slave1:2181,slave2:2181"
    val zkClient = new ZkClient(zkQuorum)

    //获取子目录下的文件数量
    val childrenCount = zkClient.countChildren(offsetDir)

    //如果有文件就读去偏移量
    val result = if (childrenCount > 0) {
      var offsetResult = Map[TopicPartition, Long]()
      (0 until childrenCount).foreach(f = part => {
        val offset: String = zkClient.readData[String](offsetDir + s"/${part}")
        offsetResult += (new TopicPartition(topic, part) -> offset.toLong)
      })

      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsetResult)
      )
      //没用则从头开始读
    } else {
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
      )
    }

    result.foreachRDD(rdd => {
      val ranges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      for (i <- ranges) {
        println(i.topic + "-" + i.partition + "-" + i.untilOffset + "-" + topic)
        //将偏移量写入zookeeper上
        ZkUtils.updateEphemeralPath(zkClient, offsetDir + "/" + i.partition, i.untilOffset.toString)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
