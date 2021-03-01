package io.wrp.batch.source

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * @author DELL
 * @date 2021/3/1 17:29
 * @description 集合数据源
 */
object CollectionSource {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val collectSet: DataSet[String] = env.fromCollection(Array("Flink source collection",
      "Flink Spark"))
    val result = collectSet.flatMap(x=>x.split(" ")).map(x => (x, 1))
      .groupBy(0).sum(1)
    result.setParallelism(1).writeAsCsv("D:\\tmp\\result1.txt")
    env.execute()
  }
}
