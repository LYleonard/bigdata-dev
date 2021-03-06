package io.wrp.windows

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer
import scala.util.Sorting

/**
  * @author LYleonard
  * @date 2021/2/25 15:09
  * @description TODO
  */
object FlinkWaterMark2 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    //设置flink的数据处理时间为 event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val tupleStream: DataStream[(String, Long)] = env.socketTextStream("master", 9000).map(x => {
      val strings: Array[String] = x.split(" ")
      (strings(0), strings(1).toLong)
    })

    //注册watermark
    val waterMarkStream = tupleStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
      var currentTimemillis: Long = 0L
      var timeDiff: Long = 10000L
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      /**
        * 获取当前数据的 watermaker
        * @return
        */
      override def getCurrentWatermark: Watermark = {
        val watermark = new Watermark(currentTimemillis-timeDiff)
        watermark
      }

      /**
        * 抽取数据的event time
        * @param element
        * @param previousElementTimestamp
        * @return
        */
      override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
        val eventTime = element._2
        currentTimemillis = Math.max(eventTime, currentTimemillis)
        val id = Thread.currentThread().getId
        println("currentThreadId:" + id + ", key:" + element._1 + ", eventTime:[" + element._2 + "|" + sdf.format(element._2) +
        "], currentMaxTimestamp:[" + currentTimemillis + "|" + sdf.format(currentTimemillis) + "], watermark:[" +
          this.getCurrentWatermark.getTimestamp + "|" + sdf.format(this.getCurrentWatermark.getTimestamp) + "]")
        eventTime
      }
    })

    waterMarkStream.keyBy(0)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//      .allowedLateness(Time.seconds(2)) //允许数据迟到2秒
      .apply(new MyWindowFunction).print()
    env.execute()
  }
}

class MyWindowFunction extends WindowFunction[(String, Long), String, Tuple, TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
    val keyStr = key.toString
    val arrBuf = ArrayBuffer[Long]()
    val ite = input.iterator
    while (ite.hasNext){
      val tup2 = ite.next()
      arrBuf.append(tup2._2)
    }
    val arr = arrBuf.toArray
    Sorting.quickSort(arr)//对数据进行排序，按照event time排序
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val result = "聚合数据的key为："+keyStr + "," +
      " 窗口当中数据的条数为："+arr.length + "," +
      " 窗口当中第一条数据为："+sdf.format(arr.head) + "," +
      " 窗口当中最后一条数据为："+ sdf.format(arr.last)+ "," +
      " 窗口起始时间为："+sdf.format(window.getStart) + "," +
      " 窗口结束时间为："+sdf.format(window.getEnd)  + "！！！！！看到这个结果，就证明窗口已经运行了"
    out.collect(result)
  }
}
