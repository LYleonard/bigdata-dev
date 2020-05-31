package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author LYleonard
  * @Date 2020/6/1 0:50
  * @Description SparkStreaming整合SparkSQL
  */
object StreamingAndSQL {
  def main(args: Array[String]): Unit = {
    Logger.getLogger(this.getClass).setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("SparkStreamingAndSparkSQL").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val socketTextStream = ssc.socketTextStream("master", 9999)
    val words = socketTextStream.flatMap(_.split(" "))

    //对DStream进行处理，将RDD转换成DataFrame
    words.foreachRDD(rdd => {
      val sparkSession = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()

      import sparkSession.implicits._
      val dataFrame = rdd.toDF("word")
      //注册为表
      dataFrame.createOrReplaceTempView("words")
      val result = sparkSession.sql("select word, count(*) as count from words " +
        "group by word")

      result.show()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
