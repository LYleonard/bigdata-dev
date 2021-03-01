package webclicks

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author LYleonard
  * @Date 2020/5/10 23:31
  * @Description spark程序对点击流日志数据进行分析
  *             TopN(求访问次数最多的URL前N位)
  */
object TopN {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("TopN").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val dataRDD = sc.textFile("E:\\develop\\Java\\bigdata-dev\\" +
      "spark_dev\\src\\test\\scala\\data\\access.log")
    val filtedRDD = dataRDD.filter(_.split(" ").length > 10)
    val urlRDD = filtedRDD.map(_.split(" ")(10))

    val urlCount: RDD[(String, Int)] = urlRDD.map(x => (x, 1)).reduceByKey(_ + _)
    val sortedResult: RDD[(String, Int)] = urlCount.filter(x => x._1 != "\"-\"")
      .sortBy(_._2, ascending = false)
    val top5 = sortedResult.take(5)

    top5.foreach(println)

    sc.stop()
  }
}
