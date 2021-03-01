package mysqlops

import java.sql.{Connection, DriverManager}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author LYleonard
  * @Date 2020/5/11 9:31
  * @Description TODO
  */
object Data2MysqlForeachPartition {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Data2MysqlForeachPartition").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val personRDD = sc.textFile("E:\\tmp\\data\\person.txt")
    val personTuple = personRDD.map(_.split(",")).map(x => (x(0), x(1), x(2)))
    personTuple.foreachPartition(iter => {
      var connection: Connection = null
      try {
        connection = DriverManager.getConnection("jdbc:mysql://192.168.29.2:3306/userdb",
          "root", "243015")
        val sql = "insert into person(id, name, age) values(?,?,?)"
        val ps = connection.prepareStatement(sql)

        iter.foreach(t => {
          ps.setString(1, t._1)
          ps.setString(2, t._2)
          ps.setInt(3, t._3.toInt)
          //设置批量提交
          ps.addBatch()
        })
        //执行sql语句
        ps.executeBatch()
      } catch {
        case e: Exception => println(e.getMessage)
      } finally {
        if (connection != null){
          connection.close()
        }
      }
    })
    sc.stop()
  }
}
