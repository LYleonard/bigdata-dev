package io.wrp.table

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}

/**
 * @author DELL
 * @date 2021/3/4 17:06
 * @description FlinkSQL实现读取CSV文件数据，并进行查询
 */
object StreamSQLCsv {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    // 新版本，基于blink planner的批处理
    val bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner()
      .inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(streamEnv, bsSettings)
//    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(streamEnv)

    tableEnv.connect(new FileSystem().path("D:\\test\\product.csv"))
      // new OldCsv()是一个非标的格式描述
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.INT())
        .field("name",DataTypes.STRING())
        .field("company", DataTypes.STRING())
      )
      .createTemporaryTable("inputTable")

    val inputTable: Table = tableEnv.from("inputTable")
    val result = inputTable.filter("id >= 2")
    val resultDS: DataStream[Product] = tableEnv.toAppendStream(result)
    resultDS.print()
    streamEnv.execute()
  }
}

case class Product(id: Int, name: String, companyName: String)
