package io.wrp.sources

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @author LYleonard
  * @date 2020-6-21 16:52
  * @description 使用自定义数据源
  **/
object MyParallelSourceImp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val data = env.addSource(new MyParallelSource)
    val result = data.flatMap(_.split(" ")).map((_, 1))
      .keyBy(0).sum(1)

    result.print().setParallelism(2)

    env.execute()
  }
}
