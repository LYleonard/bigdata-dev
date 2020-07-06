package io.wrp.operators

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @author LYleonard
  * @date 2020/7/6 21:57
  * @description 对filter过后的数据进行重新分区
  */
object FlinkPartitionAfterFilter {
  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val dataStream = environment.fromElements("hello word", "test spark", "hello", "hello Flink")

    val partitionedStream = dataStream.filter(x => x.contains("hello"))
      //      .shuffle //随机重分区，将上游数据随机发送到下游的分区中
      //      .rescale
      .rebalance //对数据重新进行分区，涉及到shuffle过程
      .flatMap(_.split(" "))
      .map(x => (x, 1))
      .keyBy(0).sum(1)

    partitionedStream.print().setParallelism(1)
    environment.execute()
  }
}
