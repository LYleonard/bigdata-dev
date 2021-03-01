package io.wrp.batch.source

import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author DELL
 * @date 2021/3/1 17:06
 * @description 从文件读入数据
 */
object FileSource {
  def main(args: Array[String]): Unit = {
    val inputPath = "D:\\test"
    val output = "D:\\tmp\\result"
    val config = new Configuration()
    config.setBoolean("recursive.file.enumeration", true)
    val env = ExecutionEnvironment.getExecutionEnvironment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setRuntimeMode(RuntimeExecutionMode.BATCH)

    val text: DataSet[String] = env.readTextFile(inputPath).withParameters(config)

    import org.apache.flink.api.scala._
    val value: AggregateDataSet[(String, Int)] = text.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .groupBy(0) //按第一列分组
      .sum(1)  // 按第二列求和
    value.setParallelism(1).writeAsCsv(output)
    env.execute()
  }
}
