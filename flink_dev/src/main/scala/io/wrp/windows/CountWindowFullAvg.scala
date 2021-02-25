package io.wrp.windows

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector

/**
  * @author LYleonard
  * @date 2021/2/25 13:13
  * @description Count window 全量聚合统计，求平均3条数据平均值
  */
object CountWindowFullAvg {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val source: DataStream[String] = env.socketTextStream("master", 9000)
    val avgByWindow = source.map(x => (1, x.trim().toInt)).keyBy(0)
//      .timeWindow(Time.seconds(5))
      .countWindow(3)
      .process(new MyProcessWindowFunction).print()

    env.execute("CountWindowFullAvg")
  }
}

/**
  * ProcessWindowFunction需要4个参数
  * 输入参数类型，输出参数类型，聚合key的类型，window的下界
  */
class MyProcessWindowFunction extends ProcessWindowFunction[(Int, Int), Double, Tuple, GlobalWindow]{
  override def process(key: Tuple, context: Context, elements: Iterable[(Int, Int)], out: Collector[Double]): Unit = {
    var totalNum = 0
    var countNum = 0
    for (data <- elements) {
      totalNum += 1
      countNum += data._2
    }
    out.collect(countNum/totalNum.toDouble)
  }
}
