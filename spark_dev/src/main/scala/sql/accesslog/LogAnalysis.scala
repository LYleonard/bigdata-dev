package sql.accesslog

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @Author LYleonard
  * @Date 2020/5/16 16:04
  * @Description 日志分析案例
  */
object LogAnalysis {

  //mysql连接信息
  val url = "jdbc:mysql://192.168.29.2:3306/accesslog"
  val properties = new Properties()
  properties.setProperty("user", "root")
  properties.setProperty("password", "243015")

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AccessLogAnalysis")
      .master("local[*]").getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val logRDD: RDD[String] = sc.textFile("./spark_dev/src/test/scala/data/access.log")

    //过滤脏数据
    val logClean: RDD[String] = logRDD.filter(line => AccessLogUtils.isValidateLogLine(line))
    val accessLogRDD: RDD[AccessLog] = logClean.map(line => AccessLogUtils.parseLogLine(line))

    // 将RDD转换成DataFrame
    import spark.implicits._
    val accessLogDF = accessLogRDD.toDF()
//    accessLogDF.show()
    //创建为临时视图
    accessLogDF.createTempView("accesslog")

    //使用SparkSQL分析日志数据
    //求contentSize的平均值，最大值以及最小值
    val contentSizeResult = spark.sql(
      """
        |select date_sub(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'),1) as time,
        |avg(contentSize) as avg_contentSize,
        |max(contentSize) as max_contentSize,
        |min(contentSize) as min_contentSize
        |from accesslog
      """.stripMargin)
    contentSizeResult.show()
    contentSizeResult.write.mode(SaveMode.Overwrite).jdbc(url, "t_contentSizeInfo", properties)

    // 计算PV and UV
    val pvuvDF = spark.sql(
      """
        |select date_sub(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), 1) as time,
        |count(*) as pv,
        |count(distinct ipAddress) as uv
        |from accesslog
      """.stripMargin)
    pvuvDF.show()
    pvuvDF.write.mode(SaveMode.Overwrite).jdbc(url, "t_pvuv", properties)

    //求各个响应码出现的次数
    val responseCode = spark.sql(
      """
        |select
        |date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'),1) as time,
        |responseCode as code,
        |count(*) as count
        |from accesslog
        |group by responseCode
      """.stripMargin)
    responseCode.show()
    responseCode.write.mode(SaveMode.Overwrite).jdbc(url, "t_responseCode", properties)

    //求访问url次数最多的前5位
    val accessUrlTopN = spark.sql(
      """
        |select *, date_sub(from_unixtime(unix_timestamp(), 'yyyy-MM-dd'), 1) as time
        |from (select url as url, count(*) as count from accesslog group by url) t
        |order by t.count desc limit 5
      """.stripMargin)
    accessUrlTopN.show()
    accessUrlTopN.write.mode(SaveMode.Overwrite).jdbc(url, "t_accessUrlTopN", properties)

    // 求各个请求方式出现的次数
    val accessMethod = spark.sql(
      """
        |select date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) as time,
        |method, count(*) as count from accesslog
        |group by method
      """.stripMargin)
    accessMethod.show()
    accessMethod.write.mode(SaveMode.Overwrite).jdbc(url, "t_accessMethod", properties)

    spark.stop()
  }
}
