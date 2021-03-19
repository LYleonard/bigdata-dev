package io.wrp.table

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.sinks.CsvTableSink

/**
 * @author DELL
 * @date 2021/3/18 14:22
 * @description TODO
 */
object FlinkStreamSQL {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)

    import org.apache.flink.api.scala._

    //101,zhangsan,18
    //102,lisi,20
    //103,wangwu,25
    //104,zhaoliu,8
    val socket: DataStream[String] = env.socketTextStream("hadoop01", 9999)
    val userStream: DataStream[User] = socket.map(x=> User(
      x.split(",")(0).toInt, x.split(",")(1), x.split(",")(2).toInt
    ))

    tableEnv.createTemporaryView("userTable", userStream)
//    val table = tableEnv.scan("userTable").filter("age > 10")
    val table: Table = tableEnv.sqlQuery("select * from userTable where age > 20")
    val sink: CsvTableSink = new CsvTableSink("D:\\test\\flink\\sink.csv",
      "|", 1, WriteMode.OVERWRITE)

    val result: DataStream[User] = tableEnv.toAppendStream(table)
    result.print()
    env.execute()
  }
}

case class User(id: Int, name: String, age: Int)
