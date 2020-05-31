package streaming.checkpoint

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author LYleonard
  * @Date 2020/5/31 22:26
  * @Description 通过checkpoint可以实现Driver端的高可用
  */
object DriverHAWC {

  val checkpointPath = "hdfs://master:8020/spark/streaming/driverck"

  /**
    * @param currentValue: 当前批次中每一个单词出现的所有的1
    * @param historyValues: 之前批次中每个单词出现的总次数,Option类型表示存在或者不存在。
    *                     Some表示存在有值，None表示没有
    * @return
    */
  def updateFunc(currentValue: Seq[Int], historyValues:Option[Int]): Option[Int] = {
    val newValue = currentValue.sum + historyValues.getOrElse(0)
    Some(newValue)
  }

  def creatingFunc(): StreamingContext = {
    Logger.getLogger("oeg").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("DriverHAWC").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint(checkpointPath)
    val socketTextStream = ssc.socketTextStream("master", 9999)
    val result = socketTextStream.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunc)
    result.print()
    ssc
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("oeg").setLevel(Level.ERROR)
    val ssc = StreamingContext.getOrCreate(checkpointPath, creatingFunc)

    ssc.start()
    ssc.awaitTermination()
  }
}
