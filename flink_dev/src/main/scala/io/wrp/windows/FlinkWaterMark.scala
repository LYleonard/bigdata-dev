package io.wrp.windows

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author LYleonard
  * @date 2021/2/25 17:07
  * @description TODO
  */
object FlinkWaterMark {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    import org.apache.flink.api.scala._

    val text: DataStream[String] = env.socketTextStream("master", 9000)
    val inputMap: DataStream[(String, Long)] = text.map(line => {
      val arr: Array[String] = line.split(" ")
      (arr(0), arr(1).toLong)
    })

    val watermarkStream: DataStream[(String, Long)] = inputMap.assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks[(String, Long)] {
      var currentMaxTimestamp: Long = 0L
      //watermark基于eventTime向后推迟10秒钟，允许消息最大乱序时间为10s
      var waterMarkDiff: Long = 10000L
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def checkAndGetNextWatermark(lastElement: (String, Long), extractedTimestamp: Long): Watermark = {
        val watermark = new Watermark(currentMaxTimestamp - waterMarkDiff)
        watermark
      }

      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
        val eventTime: Long = element._2
        currentMaxTimestamp = Math.max(eventTime, currentMaxTimestamp)
        val id: Long = Thread.currentThread().getId
        println("currentThreadId:"+id+",key:"+
          element._1+",eventtime:["+element._2+"|"+
          sdf.format(element._2)+"],currentMaxTimestamp:["+currentMaxTimestamp+"|"+
          sdf.format(currentMaxTimestamp)+"],watermark:["+this.checkAndGetNextWatermark(element,previousElementTimestamp).getTimestamp+"|"+
          sdf.format(this.checkAndGetNextWatermark(element,previousElementTimestamp).getTimestamp)+"]")
        eventTime
      }
    })

    val outputTag: OutputTag[(String, Long)] = new OutputTag[(String, Long)]("late_data")
    val outputWindow = watermarkStream.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(3)))
//      .allowedLateness(Time.seconds(2))
      .sideOutputLateData(outputTag).apply(new MyWindowFunction)

    val sideOutput: DataStream[(String, Long)] = outputWindow.getSideOutput(outputTag)
    sideOutput.print()
    outputWindow.print()

    env.execute()
  }
}
