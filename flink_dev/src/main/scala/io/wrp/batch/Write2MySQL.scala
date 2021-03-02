package io.wrp.batch

import java.sql.PreparedStatement

import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * @author DELL
 * @date 2021/3/1 17:48
 * @description 写数据到MySQL
 */
object Write2MySQL {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val source = env.fromElements("1,flink", "2,spark", "3,hive")
    val dataSet = source.mapPartition(part => {
      Class.forName("com.mysql.cj.jdbc.Driver").newInstance()
      val conn = java.sql.DriverManager.getConnection(
        "jdbc:mysql://localhost:3306/test?useSSL=false&serverTimezone=GMT%2B8&useUnicode=true&characterEncoding=UTF-8",
        "root", "123456")
      part.map(x => {
        val statement: PreparedStatement = conn.prepareStatement("insert into user1 (id,name) values(?,?)")
        statement.setInt(1, x.split(",")(0).toInt)
        statement.setString(2, x.split(",")(1))
        statement.execute()
      })
    }).print()

    env.execute()
  }
}
