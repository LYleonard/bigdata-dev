package sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.types.StringType

/**
  * @Author LYleonard
  * @Date 2020/5/16 0:21
  * @Description 用户自定义函数
  */
object SqlUDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("UDF")
      .master("local[*]").getOrCreate()

    val udfDF = spark.read.text("E:\\tmp\\data\\test_udf_data.txt")

    udfDF.createTempView("t_udf")

    spark.udf.register("lower2upper", new UDF1[String, String](){
      override def call(t1: String): String = {
        t1.toUpperCase
      }
    }, StringType)

    spark.udf.register("upper2lower", (x: String) => x.toLowerCase)

    spark.sql("select * from t_udf").show()
    spark.sql("select lower2upper(value) from t_udf").show()
    spark.sql("select upper2lower(value) from t_udf").show()

    spark.stop()
  }
}
