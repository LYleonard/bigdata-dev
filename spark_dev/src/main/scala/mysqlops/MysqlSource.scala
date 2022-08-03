package mysqlops

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._


/**
  * @author LYleonard
  * @date 2022/5/31 22:14
  * @description TODO
  */
object MysqlSource {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("source_from_mysql").getOrCreate()

    //导入隐式转换
    import sparkSession.implicits._

    val url = "jdbc:mysql://localhost:3306/dwd?useUnicode=true&characterEncoding=utf-8"
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "243015")

    val data: DataFrame = sparkSession.read.jdbc(url = url, table = "order", prop)

    val removeQuotation: UserDefinedFunction = udf((field: String) => {
      field.replaceAll("\"","")
    })

    val filterData = data.select($"userId", $"productId").filter($"productId" =!= null).withColumn("userId", removeQuotation(col("userId")))

    filterData.show()


  }

}
