package streaming.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author LYleonard
  * @Date 2020/6/4 17:03
  * @Description
  * * 使用直连方式 SparkStreaming连接kafka0.8获取数据，手动将偏移量数据保存到zookeeper中
  */
object KafkaManagerOffset08 {

  /**
    * 获取zk节点上的子节点的个数
    * @param zkClient
    * @param zkTopicPath
    * @return
    */
  def getZkChildrenNum(zkClient: ZkClient, zkTopicPath: String): Int = {
    //查询该路径下是否有子节点，即是否有分区读取数据记录的读取的偏移量
    // /consumers/consumer-sparkStreaming/offsets/wordcount/0
    // /consumers/consumer-sparkStreaming/offsets/wordcount/1
    val childrenNum = zkClient.countChildren(zkTopicPath)
    childrenNum
  }

  def main(args: Array[String]): Unit = {
    //1、创建SparkConf 提交到集群中运行 不要设置master参数
    val sparkConf = new SparkConf().setAppName("KafkaManagerOffset08").setMaster("local[*]")
    //2、设置SparkStreaming，并设定间隔时间
    val ssc = new StreamingContext(sparkConf, Seconds(4))

    //3、指定设置相关参数
    val  groupId = "consumer-sparkStreaming" //指定组名
    val topic = "test"   //指定消费者要消费的topic名字
    val brokerList = "master:9092,slave1:9092,slave2:9092"    //指定kafka的broker地址
    //zookeeper地址，用于存放消费偏移量，也可以使用mysql，redis
    val zkQuorum = "master:2181,slave1:2181,slave2:2181"
    //创建Stream时使用的topic名字集合，SparkStreaming可同时消费多个topic
    val topics = Set(topic)
    //创建一个 ZKGroupTopicDirs 对象，就是用来指定在zk中的存储目录，用来保存数据偏移量
    val topicDir = new ZKGroupTopicDirs(groupId, topic)
    //获取 zookeeper 中的路径 "/consumers/consumer-sparkStreaming/offsets/wordcount"
    val zkTopicPath = topicDir.consumerOffsetDir
    //构造一个zookeeper客户端对象，用来读取偏移量
    val zkClient = new ZkClient(zkQuorum)

    //设置kafka参数
    val kafkaParams = Map(
      "metadata.broker.list" -> brokerList,
      "group.id" -> groupId,
      "enable.auto.commit" -> "false"
    )

    //4. 定义kafkaStream流
    var kafkaDStream: InputDStream[(String, String)] = null
    //5、获取指定的zk节点的子节点个数
    val childNum = getZkChildrenNum(zkClient, zkTopicPath)

    // 6、判断是否保存过数据 根据子节点的数量是否为0
    if (childNum > 0){
      //构造一个map集合存放数据偏移量信息
      var fromOffsets: Map[TopicAndPartition, Long] = Map()
      //遍历子节点
      for (i <- 0 until childNum){
        //获取子节点,如/consumers/consumer-sparkStreaming/offsets/wordcount/0
        val partitionOffset: String = zkClient.readData[String](s"$zkTopicPath/$i")
        // 获取topic的partition
        val tp: TopicAndPartition = TopicAndPartition(topic, i)
        //获取数据偏移量， 将不同分区内的数据偏移量保存在map集合中
        fromOffsets += (tp -> partitionOffset.toLong)
      }

      //泛型中，key：kafka中的key，value：消息的内容
      // 创建函数 解析数据 组成（topic_name，message）的元组
      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())

      //利用底层的API创建DStream采用直接连接的方式（已消费过的，从指定位置消费）
      kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder,
        (String, String)](ssc,kafkaParams,fromOffsets, messageHandler)
    } else {
      //利用底层的API创建DStream 采用直连的方式(之前没有消费，一次读取数据)
      //zk中没有子节点数据 就是第一次读取数据 直接创建直连对象
      kafkaDStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
    }

    //直接操作kafkaStream
    //  依次迭代DStream中的kafkaRDD 只有kafkaRDD才可以强转为HasOffsetRanges  从中获取数据偏移量信息
    //  之后是操作的RDD 不能够直接操作DStream 因为调用Transformation方法之后就不是kafkaRDD了获取不了偏移量信息
    kafkaDStream.foreachRDD(kafkaRDD => {
      //强转为HasOffsetRanges 获取offset偏移量数据
      val offsetRanges: Array[OffsetRange] = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges
      val lines: RDD[String] = kafkaRDD.map(_._2)

      //对RDD进行操作 触发action
      lines.foreachPartition(partition => {
        partition.foreach(x=>println(x))
      })

      //手动将偏移量提交到zk上
      for (offsets <- offsetRanges) {
        //拼接offset在zk中的路径
        val zkPath = s"${topicDir.consumerOffsetDir}/${offsets.partition}"

        //将partition的偏移量数据offse保存到zookeeper中
        ZkUtils.updatePersistentPath(zkClient, zkPath, offsets.untilOffset.toString)
      }
    })

    //开启SparkStreaming 并等待退出
    ssc.start()
    ssc.awaitTermination()
  }
}
