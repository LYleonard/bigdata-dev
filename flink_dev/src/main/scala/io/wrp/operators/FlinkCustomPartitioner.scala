package io.wrp.operators

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @author LYleonard
  * @date 2020/7/6 22:25
  * @description 根据自定义分区类分区
  */
object FlinkCustomPartitioner {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    import org.apache.flink.api.scala._
    val sourceStream = env.fromElements("hello world", "spark flink", "hello world",
      "hive hadoop")

    val repartition = sourceStream.partitionCustom(new MyPartitioner, x => x + "")
    repartition.map(x => {
      println("数据的key为：" + x + ", 线程为：" + Thread.currentThread().getId)
      x
    })
    repartition.print()
    env.execute()
  }
}
