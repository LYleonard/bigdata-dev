package io.wrp.operators

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
  * @author LYleonard
  * @date 2020-6-21 17:30
  * @description union transformations Operators 实现相同类型DataStream连接
  **/
object UnionStreamOps {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val stream1: DataStream[String] = env.fromElements("hello world", "test scala")
    val stream2: DataStream[String] = env.fromElements("hello flink", "flink operator")
    val stream: DataStream[String] = stream1.union(stream2)

    val result = stream.map(x => x)

    result.print().setParallelism(1)
    env.execute()
  }
}
