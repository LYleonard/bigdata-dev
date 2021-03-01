package streaming.flume

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author LYleonard
  * @Date 2020/6/7 16:00
  * @Description  Approach 1: Flume-style Push-based Approach
  * *  Produces a count of events received from Flume.
  * *
  * *  This should be used in conjunction with an AvroSink in Flume. It will start
  * *  an Avro server on at the request host:port address and listen for requests.
  * *  Your Flume AvroSink should be pointed to this address.
  * *
  * *  Usage: FlumeEventCount <host> <port>
  * *    <host> is the host the Flume receiver will be started on - a receiver
  * *           creates a server and listens for flume events.
  * *    <port> is the port the Flume receiver will listen on.
  * *
  * *  To run this example:
  * *    `$ bin/spark-submit ... streaming.flume.FlumeEventCount <host> <port> `
  */
object FlumeEventCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.print("Usage: FlumeEventCount <host> <port>")
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)

    val Array(host, port) = args
    val batchInterval = Seconds(2)

    val sparkConf = new SparkConf().setAppName("FlumeEventCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, batchInterval)

    //create a flume stream
    val stream = FlumeUtils.createStream(ssc,host,port.toInt,StorageLevel.MEMORY_AND_DISK_SER_2)
    stream.count().map(cnt => "Received " + cnt + " flume event.").print()

    ssc.start()
    ssc.awaitTermination()
  }
}
