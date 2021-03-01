package sql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @Author LYleonard
  * @Date 2020/5/15 11:08
  * @Description  通过StructType动态指定Schema
  */
object StructTypeSchema {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("StructTypeSchema")
      .master("local[*]").getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val person = sc.textFile("E:\\tmp\\data\\person.txt").map(_.split(","))
    val personRDD = person.map(x => Row(x(0), x(1), x(2).toInt))

    //指定schema
    val schema = StructType(
      StructField("id", StringType)::
      StructField("name", StringType)::
      StructField("age", IntegerType)::Nil
    )

    val DF = spark.createDataFrame(personRDD,schema = schema)
    DF.printSchema()
    DF.show()

    DF.createTempView("user")
    spark.sql("select * from user").show()

    spark.close()
  }
}
