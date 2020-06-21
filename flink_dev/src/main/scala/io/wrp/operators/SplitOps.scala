package io.wrp.operators

import java.{lang, util}

import org.apache.flink.streaming.api.collector.selector.OutputSelector
import org.apache.flink.streaming.api.scala.{SplitStream, StreamExecutionEnvironment}

/**
  * @author LYleonard
  * @date 2020-6-21 17:55
  * @description transformation operator: split, 将一个DataStream且分为多个DataStream
  **/
object SplitOps {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val raw = env.fromElements("hello flink", "spark kafka", "flink kafka")

    val splitedDS: SplitStream[String] = raw.split(new OutputSelector[String] {
      override def select(out: String): lang.Iterable[String] = {
        val strings = new util.ArrayList[String]()
        if (out.contains("hello")){
          strings.add("hello")
        } else {
          strings.add("others")
        }
        strings
      }
    })

    val result = splitedDS.select("hello")
    result.print().setParallelism(1)
    env.execute()
  }
}
