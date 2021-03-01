package io.wrp.operators

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

/**
  * @author LYleonard
  * @date 2020-6-21 17:55
  * @description transformation operator: split, 将一个DataStream且分为多个DataStream
  **/
object SplitOps {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val raw: DataStream[String] = env.fromElements("hello flink", "spark kafka", "flink kafka")


/*    //1.8.1版本Flink Split算子实现
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
*/

    // 1.12.0版本Flink 通过Side out实现
    val outputTag: OutputTag[String] = OutputTag[String]("hello")
    val result: DataStream[String] = raw.process(new ProcessFunction[String, String] {
      override def processElement(value: String,
                                  ctx: ProcessFunction[String, String]#Context,
                                  out: Collector[String]): Unit = {

        if (value.contains("hello")){
          out.collect(value)
        } else {
          ctx.output(outputTag,value)
        }
      }
    })
    val siderOutPut: DataStream[String] = result.getSideOutput(outputTag)
    siderOutPut.print("side out")
    result.print("hello")

    env.execute()
  }
}
