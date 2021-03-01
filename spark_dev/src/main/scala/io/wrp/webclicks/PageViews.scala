package webclicks

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author LYleonard
  * @Date 2020/5/10 23:09
  * @Description spark程序对点击流日志数据进行分析
  */
object PageViews {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("PageView").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val dataRDD: RDD[String] = sc.textFile("E:\\develop\\Java\\bigdata-dev\\" +
      "spark_dev\\src\\test\\scala\\data\\access.log")

    //统计PV
    val pageView: Long = dataRDD.count()
    println(s"pv: $pageView")

    sc.stop()
  }
}
