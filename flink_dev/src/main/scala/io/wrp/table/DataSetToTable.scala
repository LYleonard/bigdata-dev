package io.wrp.table

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.scala.{BatchTableEnvironment, StreamTableEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
/**
 * @author DELL
 * @date 2021/3/19 16:23
 * @description DataSet与Table的互相转换只有在older planer中实现。
 *             而Blink 将批处理作业视作流处理的一种特例。严格来说，Table 和 DataSet 之间不支持相互转换，
 *             并且批处理作业也不会转换成 DataSet 程序而是转换成 DataStream 程序，流处理作业也一样。
 */
//object DataSetToTable {
//  def main(args: Array[String]): Unit = {
//
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    val bbSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
//    val bbTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, bbSettings)
//
//    import org.apache.flink.api.scala._
//
//    val parameters = ParameterTool.fromArgs(args)
//    val inputFile = parameters.getRequired("input_file")
//    val sourceDataSet: DataStream[String] = env.readTextFile(inputFile)
//
//    val productSet: DataStream[Product] = sourceDataSet.map(p => {
//      val peoductInfo: Array[String] = p.split(",")
//      Product(peoductInfo(0).toInt, peoductInfo(1), peoductInfo(2))
//    })
//
//    import org.apache.flink.table.api._
//    bbTableEnv.fromDataStream(productSet)
//    bbTableEnv.createTemporaryView("prod", productSet)
//
//    val filterTable: Table = bbTableEnv.sqlQuery("select * from prod where id >= 2")
//    val tableToDataStream = bbTableEnv.toAppendStream[Product](filterTable)
//    tableToDataStream.print()
//    env.execute()
//
//  }
//}
object DataSetToTable {
  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = BatchTableEnvironment.create(env)

    import org.apache.flink.api.scala._

    val parameters = ParameterTool.fromArgs(args)
    val inputFile = parameters.getRequired("input_file")
    val sourceDataSet = env.readTextFile(inputFile)

    val productSet = sourceDataSet.map(p => {
      val peoductInfo: Array[String] = p.split(",")
      Product(peoductInfo(0).toInt, peoductInfo(1), peoductInfo(2))
    })

    import org.apache.flink.table.api._
    tableEnv.fromDataSet(productSet)
    tableEnv.createTemporaryView("prod", productSet)

    val filterTable: Table = tableEnv.sqlQuery("select * from prod where id >= 2")
    val tableToDataSet = tableEnv.toDataSet[Product](filterTable)
    tableToDataSet.print()
  }
}
