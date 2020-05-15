package sql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * @Author LYleonard
  * @Date 2020/5/15 14:20
  * @Description 通过 JDBC 读写Mysql
  */
object OperateMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OperateMysql")
      .master("local[*]") //本地运行，若提交到集群，要删除该配置
      .getOrCreate()

    val url = "jdbc:mysql://localhost:3306/userdb"
    val tableName = "person"
    //配置连接数据库的相关属性
    val properties = new Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "243015")
    val person = spark.read.jdbc(url, tableName, properties)

    person.printSchema()
    person.show()
    //注册成表
    person.createTempView("user")
    spark.sql("select * from user where age > 20").show()

    val schema = "id string, name string, age int"
    val result = spark.read.schema(schema).csv("E:\\tmp\\data\\person2.csv")
    result.write.mode(SaveMode.Append).jdbc(url,tableName, properties)

    spark.close()
  }
}
