package io.wrp

import org.apache.flink.api.scala.{AggregateDataSet, DataSet, ExecutionEnvironment}

/**
  * @author LYleonard
  * @date 2020/6/21 11:34
  * @description TODO
  */
object BatchOperate {
  def main(args: Array[String]): Unit = {
    val input = "E:\\tmp\\wordcout.txt"
    val output = "E:\\tmp\\out.txt"
    val env = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val text: DataSet[String] = env.readTextFile(input)
    val value: AggregateDataSet[(String, Int)] = text.flatMap(_.split(" "))
      .map((_,1)).groupBy(0).sum(1)

    value.writeAsText(output).setParallelism(1)
    env.execute("batch word count")
  }
}
