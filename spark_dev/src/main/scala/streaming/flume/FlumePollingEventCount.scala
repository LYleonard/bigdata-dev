package streaming.flume

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author LYleonard
  * @Date 2020/6/8 9:20
  * @Description Approach 2: Pull-based Approach using a Custom Sink
  *  Produces a count of events received from Flume.
  *
  *  This should be used in conjunction with the Spark Sink running in a Flume agent. See
  *  the Spark Streaming programming guide for more details.
  *
  *  Usage: FlumePollingEventCount <host> <port>
  *    `host` is the host on which the Spark Sink is running.
  *    `port` is the port at which the Spark Sink is listening.
  *
  *  To run this example:
  *    `$ bin/spark-submit ... streaming.flume.FlumePollingEventCount [host] [port] `
  */
object FlumePollingEventCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: org.apache.spark.examples.streaming.FlumePollingEventCount <host> <port>")
      System.exit(1)
    }

    Logger.getLogger("org").setLevel(Level.WARN)

    val host = args(0)
    val port = args(1).toInt
    val batchInterval = Seconds(2)

    val sparkConf = new SparkConf().setAppName("FlumePollingEventCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, batchInterval)

    // Create a flume stream that polls the Spark Sink running in a Flume agent
    val stream = FlumeUtils.createPollingStream(ssc, host, port)
    stream.count().map(cnt => "Received " + cnt + " flume events.").print()

    ssc.start()
    ssc.awaitTermination()
  }
}
