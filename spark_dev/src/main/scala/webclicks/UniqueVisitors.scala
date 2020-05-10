package webclicks

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author LYleonard
  * @Date 2020/5/10 23:22
  * @Description UV:spark程序对点击流日志数据进行分析
  */
object UniqueVisitors {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("UV").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val dataRDD: RDD[String] = sc.textFile("E:\\develop\\Java\\bigdata-dev\\" +
      "spark_dev\\src\\test\\scala\\data\\access.log")

    val ipsRDD: RDD[String] = dataRDD.map(_.split(" ")(0))
    val uv: RDD[String] = ipsRDD.distinct()
    val result: Long = uv.count()
    println(s"UV: $result")

    sc.stop()
  }
}
