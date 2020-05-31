package streaming.transformation

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{MapWithStateDStream, ReceiverInputDStream}

/**
  * @Author LYleonard
  * @Date 2020/5/31 0:42
  * @Description mapWithState实现把所有批次的单词出现的次数累加
  *             比updateStateByKey性能更好
  */
object MapWithStateWC {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("MapWithStateWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val initRDD: RDD[(String, Int)] = ssc.sparkContext.parallelize(List(("hadoop", 10),("spark", 20)))
    //需要设置checkpoint目录，用于保存之前批次的结果数据,该目录一般指向hdfs路径
    ssc.checkpoint("hdfs://master:8020/spark/streaming/ck")

    //接收socket数据
    val socketTextStream: ReceiverInputDStream[String] = ssc.socketTextStream("master", 9999)

    //数据处理
    val wordAndOneDstream = socketTextStream.flatMap(_.split(" ")).map((_, 1))

    val stateSpec = StateSpec.function((time:Time, key:String,
                                        currentVlaue:Option[Int], historyState:State[Int]) => {

      //当前批次结果与历史批次的结果累加
      val sumValue = currentVlaue.getOrElse(0) + historyState.getOption().getOrElse(0)
      val output = (key, sumValue)

      if (!historyState.isTimingOut()){
        historyState.update(sumValue)
      }
      Some(output)
    }).initialState(initRDD) //给一个初始的结果initRDD
      .timeout(Durations.seconds(30)) //当一个key超过该时间没有接收到数据时，这个key以及对应的状态会被移除掉

    val result: MapWithStateDStream[String, Int, Int, (String, Int)] = wordAndOneDstream.mapWithState(stateSpec)

    result.stateSnapshots().print()

    ssc.start()
    ssc.awaitTermination()
  }
}
