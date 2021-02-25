package io.wrp.windows

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author LYleonard
  * @date 2021/2/25 11:43
  * @description Time Window 增量聚合统计
  */
object TimeWindowIncrement {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val source = env.socketTextStream("master", 9000)
    val output = source.map(x => (1, x.toInt)).keyBy(0)
      .timeWindow(Time.seconds(5))
      .reduce(new ReduceFunction[(Int, Int)]{
        override def reduce(value1: (Int, Int), value2: (Int, Int)): (Int, Int) = {
          (value1._1,value1._2 + value2._2)
        }
      }).print()

    env.execute("Time Window Increment")
  }
}
