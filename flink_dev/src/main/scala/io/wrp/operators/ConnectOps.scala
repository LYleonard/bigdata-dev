package io.wrp.operators

import org.apache.flink.streaming.api.scala.{ConnectedStreams, DataStream, StreamExecutionEnvironment}

/**
  * @author LYleonard
  * @date 2020-6-21 17:42
  * @description connect transformations Operators 实现不相同类型DataStream连接
  **/
object ConnectOps {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val stream1: DataStream[String] = env.fromElements("hello flink", "flink operator")
    val stream2: DataStream[Int] = env.fromElements(1,2,3,4)
    val stream: ConnectedStreams[String, Int] = stream1.connect(stream2)
    val connectResult: DataStream[Any] = stream.map(x => {"word: " + x}, y => {y * 2})

    connectResult.print().setParallelism(1)
    env.execute()
  }
}
