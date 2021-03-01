package streaming.clickstream

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author LYleonard
  * @Date 2020/6/8 14:39
  * @Description
  * Analyses a streaming dataset of web page views. This class demonstrates several types of
  * operators available in Spark streaming.
  *
  * This should be used in tandem with PageViewStream.scala. Example:
  * To run the generator
  * `$ bin/run-example streaming.clickstream.PageViewGenerator 44444 10`
  * To process the generated stream
  * `$ bin/run-example \
  *     streaming.clickstream.PageViewStream errorRatePerZipCode localhost 44444`
  */
object PageViewStream {
  def main(args: Array[String]): Unit = {
    if (args.length != 3){
      System.err.println("Usage: PageViewStream <metric> <host> <port>")
      System.err.println("<metric> must be one of pageCounts, slidingPageCounts," +
        " errorRatePerZipCode, activeUserCount, popularUsersSeen")
      System.exit(1)
    }

    Logger.getLogger("org").setLevel(Level.ERROR)

    val metric = args(0)
    val host = args(1)
    val port = args(2).toInt

    val sparkConf = new SparkConf().setAppName("PageViewStream").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))

    //Create a ReceiverInputDStream on target host:port and convert each line to a PageView
    val pageView: DStream[PageView] = ssc.socketTextStream(host, port).flatMap(_.split("\n"))
      .map(PageView.fromString(_))

    // Return a count of views per URL seen in each batch
    val pageCounts = pageView.map(view => view.url).countByValue()

    // Return a sliding window of page views per URL in the last ten seconds
    val slidingPageCounts = pageView.map(view => view.url).countByValueAndWindow(Seconds(10), Seconds(2))

    // Return the rate of error pages (a non 200 status) in each zip code over the last 30 seconds
    val statusesPerZipCode = pageView.window(Seconds(30), Seconds(2))
      .map(view => (view.zipCode, view.status)).groupByKey()
    val errorRatePerZipCode = statusesPerZipCode.map{
      case(zip, statuses) => {
        val normalCount = statuses.count(_ == 200)
        val errorCount = statuses.size - normalCount
        val errorRatio = errorCount.toFloat / statuses.size
        if (errorRatio > 0.05) {
          "%s: **%s**".format(zip, errorRatio)
        } else {
          "%s: %s".format(zip, errorRatio)
        }
      }
    }

    // Return the number unique users in last 15 seconds
    val activeUserCount = pageView.window(Seconds(15), Seconds(2)).map(view => (view.userID, 1))
      .groupByKey().count().map("Unique active users: " + _)

    // An external dataset we want to join to this stream
    val userList = ssc.sparkContext.parallelize(Seq(
      1 -> "Patrick Wendell",
      2 -> "Reynold Xin",
      3 -> "Matei Zaharia"
    ))

    metric match {
      case "pageCounts" => pageCounts.print()
      case "slidingPageCounts" => slidingPageCounts.print()
      case "errorRatePerZipCode" => errorRatePerZipCode.print()
      case "activeUserCount" => activeUserCount.print()
      case "popularUsersSeen" =>
      // Look for users in our existing dataset and print it out if we have a match
        pageView.map(view => (view.userID, 1)).foreachRDD((rdd, time) =>
          rdd.join(userList).map(_._2._2).take(10)
            .foreach(u => println(s"Saw user $u at time $time"))
        )
      case _ => println(s"Invalid metric entered: $metric")
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
