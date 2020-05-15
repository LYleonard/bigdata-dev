package sql

import org.apache.spark.sql.SparkSession

/**
  * @Author LYleonard
  * @Date 2020/5/15 12:00
  * @Description Spark SQL操作Hive SQL
  */
object HiveSupport {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HiveSupport").enableHiveSupport()
      .master("local[*]").getOrCreate()

    spark.sql("create table people(id string, name string, age int) row " +
      "format delimited fields terminated by ','")
    spark.sql("load data local inpath './data/person.txt' into table people")
    spark.sql("select * from people").show()

    spark.close()
  }
}
