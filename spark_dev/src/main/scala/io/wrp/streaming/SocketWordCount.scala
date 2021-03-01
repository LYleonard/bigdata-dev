package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author LYleonard
  * @Date 2020/5/25 11:55
  * @Description sparkStreaming接受socket数据实现单词计数程序
  *             nc -lk 9999
  */
object SocketWordCount {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("SocketWordCountSparkStreaming")
      .setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    //接收socket数据
    val socketTextStream: ReceiverInputDStream[String] =
      ssc.socketTextStream("master", 9999)

    //数据处理
    val result: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" "))
      .map((_,1)).reduceByKey(_+_)

    // 结果输出
    result.print()

    //开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
