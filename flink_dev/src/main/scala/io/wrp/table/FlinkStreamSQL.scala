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

    //AppendMode
    //将表附加到流数据，表当中只能有查询或者添加操作，如果有update或者delete操作，那么就会失败
    val result: DataStream[User] = tableEnv.toAppendStream(table)

    //RetraceMode
    //始终可以使用此模式。返回值是boolean类型。
    // 它用true或false来标记数据的插入和撤回，返回true代表数据插入，false代表数据的撤回
    val result2: DataStream[(Boolean, User)] = tableEnv.toRetractStream[User](table)
    result.print()
    result2.print()
    env.execute()
  }
}

case class User(id: Int, name: String, age: Int)
