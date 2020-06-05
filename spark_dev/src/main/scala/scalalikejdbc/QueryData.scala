package scalalikejdbc

import scalikejdbc._
import scalikejdbc.config.DBs

/**
  * @Author LYleonard
  * @Date 2020/6/5 14:49
  * @Description scalikejdbc  查询数据库并封装数据
  */
object QueryData {
  def main(args: Array[String]): Unit = {
//    query()
//    queryReturnObject()
//    insertData()
    insertAndReturnKey()
  }

  def query(): Unit = {
    // 默认加载default配置信息
    DBs.setup()
    val list = DB.readOnly(implicit session=>{
      SQL("select Fname from student").map(rs => rs.string("fname")).list().apply()
    })

    for (s <- list) {
      println(s)
    }
  }

  case class Users(id: Int, name: String, age:Int)
  /**
    * 查询数据库，并将数据封装成对象，并返回一个集合
    */
  def queryReturnObject():Unit = {
    DBs.setup('stu)
    val users = NamedDB('stu).readOnly(implicit session => {
      SQL("select * from student").map(rs => {
        Users(rs.string("id").toInt, rs.string("name"), rs.string("age").toInt)
      }).list().apply()
    })

    for (u <- users){
      println(u)
    }
  }

  /**
    * 插入数据，使用AutoCommit
    */
  def insertData(): Unit = {
    DBs.setup()
    val name = DB.autoCommit(implicit session => {
      SQL("insert into student(id, name, age) values(?,?,?)").bind("4","nick","23")
        .update().apply()
    })
    println(name)
  }

  /**
    * 插入数据，使用AutoCommit
    */
  def insertAndReturnKey(): Unit = {
    DBs.setup()

    val id = "5"
    val name = "TomBill"
    val age = "25"

    val key = DB.localTx({implicit session => {
      sql"""insert into student(id, name, age) values(${id},${name},${age})"""
        .update().apply()
    }})
    println(key)
  }
}
