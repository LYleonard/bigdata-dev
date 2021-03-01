package streaming

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author LYleonard
  * @Date 2020/5/25 14:51
  * @Description 自定义一个Receiver，这个Receiver从socket中接收数据
  *             使用方式：nc -lk 9999
  */
object CustomReceiver {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("CustomReceiver").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val receiverStream = ssc.receiverStream(new CustomReceiver("master", 9999))

    val result = receiverStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)

    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}

class CustomReceiver(host: String, port: Int) extends
  Receiver[String](StorageLevel.MEMORY_AND_DISK_SER) with Logging{
  override def onStart(): Unit = {
    //启动一个线程，开始接收数据
    new Thread("socket receiver"){
      override def run(): Unit = {
        receive()
      }
    }.start()
  }
  
  private def receive(): Unit ={
    var socket: Socket = null
    var userInput: String = null
    try {
      logInfo("Connecting to " + host + ":" + port)
      socket = new Socket(host, port)
      logInfo("Connected to " + host + ":" + port)

      val reader = new BufferedReader(new InputStreamReader(
        socket.getInputStream, StandardCharsets.UTF_8))

      userInput = reader.readLine()
      while (!isStopped() && userInput !=null) {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()
      logInfo("Stopped receiving!")
      restart("Trying to connect again!")
    } catch {
      case e: java.net.ConnectException =>
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        restart("Error receiving data", t)
    }
  }

  override def onStop(): Unit = {

  }
}
