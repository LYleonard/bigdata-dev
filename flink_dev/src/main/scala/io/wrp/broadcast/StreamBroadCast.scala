package io.wrp.broadcast

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author DELL
 * @date 2021/3/3 14:43
 * @description dataStream中的广播分区
 */
object StreamBroadCast {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    import org.apache.flink.api.scala._
    val source = env.fromElements("hello flink")
    source.broadcast.map(x => {
      println(x)
      x
    })
    env.execute()
  }
}
