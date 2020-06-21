package io.wrp

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @author LYleonard
  * @date 2020-6-21 16:24
  * @description 从集合中获取数据
  **/
object ElementsSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val rawData = env.fromElements("hadoop spark", "flink HDFS flink")
    val result = rawData.flatMap(_.split(" ")).map((_, 1))
      .keyBy(0).sum(1)

    result.print().setParallelism(1)
    env.execute()
  }
}
