package mysqlops

import java.util.Properties

import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @author wrp
  * @date 2022/6/1 14:37
  * @description TODO
  */
object FeatureMatrix {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("FeatureMatrix").master("local[*]").getOrCreate()
    //隐式转换
    import sparkSession.implicits._
    val url = "jdbc:mysql://localhost:3306/dwd?characterEncoding=utf-8&serverTimezone=GMT%2B8"
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "ictbda@2018")
    val data = sparkSession.read.jdbc(url = url, table = "orders", prop).select("USERID", "PRODUCTID").filter($"PRODUCTID" =!= "null")

    val tempData = data.select($"USERID").rdd.map(_.getInt(0)).zipWithIndex().toDF("USERID", "index").join(data, Seq("USERID"), "left")

    tempData.createTempView("tmp")
    val indexDF = sparkSession.sql(
      """
        |select A.index as index_a, B.index as index_b, A.PRODUCTID as ap, B.PRODUCTID as bp from
        |tmp as A
        |JOIN
        |tmp as B
        |  on A.PRODUCTID = B.PRODUCTID
        |""".stripMargin)

    indexDF.show()

    val entities: RDD[MatrixEntry] = indexDF.select("index_a", "index_b").filter($"index_a" =!= $"index_b").rdd.map(x => {
      MatrixEntry(x.get(0).toString.toLong, x.get(1).toString.toLong, 1)
    })

    val mat: CoordinateMatrix = new CoordinateMatrix(entities)
    // Get its size.
    val m = mat.numRows()
    val n = mat.numCols()
    println(m, n)
    val rowMatrix = mat.toRowMatrix()
    rowMatrix.rows.foreach(println)

    rowMatrix.rows.saveAsTextFile("hdfs://hadoop01:8020/data/save/")

    //hdfs中显示前两行
    // hdfs dfs -cat /data/save/* | head -2

  }
}