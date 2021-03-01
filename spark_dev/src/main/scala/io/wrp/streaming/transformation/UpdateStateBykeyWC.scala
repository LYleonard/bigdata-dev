package streaming.transformation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author LYleonard
  * @Date 2020/5/28 17:58
  * @Description 实现把所有批次的单词出现的次数累加,updateStateByKey算子
  */
object UpdateStateBykeyWC {

  /**
    * @param currentValue: currentValue:当前批次中每一个单词出现的所有的1
    * @param historyValues: 之前批次中每个单词出现的总次数,Option类型表示存在或者不存在。
    *                     Some表示存在有值，None表示没有
    * @return
    */
  def updateFunc(currentValue: Seq[Int], historyValues: Option[Int]): Option[Int] = {
    val newValue: Int = currentValue.sum + historyValues.getOrElse(0)
    Some(newValue)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("UpdateStateBykeyWordCount").setMaster("local[*]")

    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(2))

    // 需要设置checkpoint目录，用于保存之前批次的结果数据,该目录一般指向hdfs路径
    ssc.checkpoint("hdfs://master:8020/spark/streaming/ck")

    //接收socket数据
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("master", 9999)

    //处理数据
    val wordAndOneDstream = socketTextStream.flatMap(_.split(" ")).map((_,1))
    val result: DStream[(String, Int)] = wordAndOneDstream.updateStateByKey(updateFunc)

    result.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
