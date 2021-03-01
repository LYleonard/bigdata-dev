package mysqlops

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author LYleonard
  * @Date 2020/5/11 8:38
  * @Description spark读取文件数据写入到mysql表,foreach算子实现
  */
object Data2MysqlForeach {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Write2MyysqlForeach").setMaster("local[*]")
    val sparkContext = new SparkContext(sparkConf)
    sparkContext.setLogLevel("WARN")

    val personRDD = sparkContext.textFile("E:\\tmp\\data\\person.txt")
    val personTuple: RDD[(String, String, Int)] = personRDD.map(_.split(","))
      .map(x => (x(0), x(1), x(2).toInt))
    personTuple.foreach(t => {
      var connection: Connection = null
      try {
        connection = DriverManager.getConnection("jdbc:mysql://192.168.29.2:3306/userdb",
          "root", "243015")
        val sql = "insert into person(id,name,age) values(?,?,?)"
        val ps: PreparedStatement = connection.prepareStatement(sql)
        ps.setString(1, t._1)
        ps.setString(2, t._2)
        ps.setInt(3, t._3)
        ps.execute()
      } catch {
        case e: Exception => println(e.getMessage)
      } finally {
        if (connection != null){
          connection.close()
        }
      }
    })
    sparkContext.stop()
  }
}
