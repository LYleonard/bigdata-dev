package streaming.transformation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author LYleonard
  * @Date 2020/5/31 11:10
  * @Description 获取每一个批次中单词出现次数最多的前3位
  */
object TransformWC {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("TransformWC").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val socketTextStream = ssc.socketTextStream("master", 9999)
    val result: DStream[(String, Int)] = socketTextStream.flatMap(_.split(" "))
      .map((_, 1)).reduceByKey(_+_)

    //将Dstream进行transform方法操作
    val sortedDStream: DStream[(String, Int)] = result.transform(rdd => {
      //排序
      val sortedRdd = rdd.sortBy(_._2, false)
      val top3 = sortedRdd.take(3)
      println("sort to start ......")
      top3.foreach(println)
      println("sorting ending ......")
      sortedRdd
    })

    sortedDStream.print()

    //开启流式计算
    ssc.start()
    ssc.awaitTermination()
  }
}
