package io.wrp.sources

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author LYleonard
  * @date 2020/6/20 17:27
  * @description TODO
  */

case class WordCount(word: String, count: Long)
object SocketSource {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //从socket当中获取数据
    val result: DataStream[String] = environment.socketTextStream("master", 9000)

    //导入隐式转换，否则时间无法使用
    import org.apache.flink.api.scala._
    val resultValue = result.flatMap(_.split(" "))
      .map(x => WordCount(x, 1)).keyBy("word")
      .timeWindow(Time.seconds(1), Time.milliseconds(1000))//按照每秒钟时间窗口，以及每秒钟滑动间隔来进行数据统计
      .sum("count")

    resultValue.print().setParallelism(1)

    environment.execute()
  }
}
