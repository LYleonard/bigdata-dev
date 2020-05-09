import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author LYleonard
  * @Date 2020/5/9 9:03
  * @Description 本地运行
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //1、构建sparkConf对象 设置application名称和master地址
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount").setMaster("local[4]")

    //2、构建sparkContext对象,该对象非常重要，它是所有spark程序的执行入口
    // 它内部会构建  DAGScheduler和 TaskScheduler 对象
    val sc: SparkContext = new SparkContext(sparkConf)

    //设置日志级别
    sc.setLogLevel("WARN")

    //读取文件
    val data: RDD[String] = sc.textFile("E:\\downloads\\wordcount.txt")

    val count: RDD[(String, Int)] = data.flatMap(_.replaceAll("[,.!?\\-]", "").
      split(" ")).map((_, 1)).reduceByKey(_+_)

    val sortedRDD: RDD[(String, Int)] = count.sortBy(x => x._2, ascending = false)
    val result: Array[(String, Int)] = sortedRDD.collect()
    result.foreach(println)

    sc.stop()
  }
}
