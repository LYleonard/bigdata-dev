package mysqlops
import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import scala.util.control.Breaks

/**
  * @author LYleonard
  * @date 2022/6/6 21:32
  * @description 特征工程-特征矩阵DataFrame格式形式【header为用户id，第一列（user）为用户id】
  *              +----+---+- -+---+---+---+
  *              |user|  1|  3|  5|  4|  2|
  *              +----+---+- -+---+---+---+
  *              |   1|  0|  1|  1|  1|  1|
  *              |   3|  1|  0|  1|  1|  1|
  *              |   5|  1|  1|  0|  1|  1|
  *              |   4|  1|  1|  1|  0|  1|
  *              |   2|  1|  1|  1|  1|  0|
  *              +----+---+- -+---+---+---+
  */
object FeatureMatrix2 {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("FeatureMatrix").master("local[*]").getOrCreate()
    //隐式转换
    import sparkSession.implicits._
    val url = "jdbc:mysql://localhost:3306/dwd?characterEncoding=utf-8&serverTimezone=GMT%2B8"
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "ictbda@2018")
    val data = sparkSession.read.jdbc(url = url, table = "orders", prop).select("USERID", "PRODUCTID").filter($"PRODUCTID" =!= "null")

    //    data.show()
    data.createTempView("tmp")

    val indexDF = sparkSession.sql(
      """
        |select A.USERID as user, B.USERID as user_b, A.PRODUCTID as ap, B.PRODUCTID as bp from
        |tmp as A
        |JOIN
        |tmp as B
        |	on A.PRODUCTID = B.PRODUCTID
        |""".stripMargin)

    indexDF.show(150)
    indexDF.select($"user").distinct().show(15)

    var df = indexDF.select($"user").distinct()
    val columns: Seq[String] = df.map(_.toString().replace("[", "").replace("]","")).collect().toList

    for (col_name <- columns){
      df = df.withColumn(col_name, lit(0))
    }

    val entities = indexDF.select($"user".as("user_a"), $"user_b").filter($"user_a" =!= $"user_b").rdd.map(x => {
      (x.get(0),x.get(1))
    })

    val indexPair = entities.collect()


    def compair(line:Row):Row={
      val rowNo:Int = line.getAs("user")
      var tmp: Seq[Int] = Seq(rowNo)
      for (col_name <- columns){
        var findFlag = false
        val loop = new Breaks;
        loop.breakable {
          for (pair <- indexPair) {
            if (pair._2.toString == col_name && pair._1.toString==rowNo.toString) {
              tmp ++= Seq(1)
              findFlag = true
              loop.break;
            }
          }
        }
        if (!findFlag){
          tmp ++=Seq(0)
        }
      }
      Row.fromSeq(tmp)
    }

    val mat: RDD[Row] = df.rdd.map(compair)

    val schema = StructType(
      Seq(StructField("user",IntegerType,true))++
        columns.map(f=>StructField(f,IntegerType,true))
    )
    val matDF = sparkSession.createDataFrame(mat, schema)
    matDF.show()

    matDF.repartition(1).write.format("csv").option("header","true").mode("append").save("E:\\tmp\\files")

  }
}