package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author LYleonard
  * @Date 2020/5/25 14:38
  * @Description sparkStreaming监控hdfs上的目录，有新文件产生，就拉取数据进行计算
  */
object HdfsWordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("SparkStreamingHDFSWC").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val textFileStream: DStream[String] = ssc.textFileStream("hdfs://master:8020/test/data")
    val result = textFileStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
