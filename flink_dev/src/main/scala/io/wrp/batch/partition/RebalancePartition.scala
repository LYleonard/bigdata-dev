package io.wrp.batch.partition

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * @author DELL
 * @date 2021/3/2 10:54
 * @description 对数据集进行再平衡，重分区，消除数据倾斜
 */
object RebalancePartition {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    import org.apache.flink.api.scala._

    val sourceDataSet: DataSet[String] = env.fromElements("hello world","hello flink","hive sqoop")
    sourceDataSet.filter(x=>x.contains("hello")).rebalance().print()

    env.execute()
  }
}
