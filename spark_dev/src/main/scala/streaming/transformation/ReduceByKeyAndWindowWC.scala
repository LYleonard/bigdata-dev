package streaming.transformation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author LYleonard
  * @Date 2020/5/31 18:55
  * @Description Window操作, 实现每隔4秒统计6秒的数据,reduceByKeyAndWindow
  */
object ReduceByKeyAndWindowWC {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("ReduceByKeyAndWindowWC").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val socketTextStream = ssc.socketTextStream("master", 9999)
    val result = socketTextStream.flatMap(_.split(" ")).map((_, 1))

    //每隔4秒统计6秒的数据
    /**
      *  1. reduceFunc： associative and commutative reduce function
      *  2. windowDuration： width of the window; must be a multiple of this DStream's
      *     batching interval.窗口的大小(时间单位)，该窗口会包含N个批次的数据
      *  3. slideDuration：  sliding interval of the window (i.e., the interval after which
      *     the new DStream will generate RDDs); must be a multiple of this DStream's batching
      *     interval.滑动窗口的时间间隔，表示每隔多久计算一次
      */
    val windowDStream: DStream[(String, Int)] = result.reduceByKeyAndWindow((x:Int, y:Int) => x+y,
      Seconds(6), Seconds(4))

    windowDStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
