package io.wrp

import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * @author DELL
 * @date 2021/3/3 16:32
 * @description Counter 累加器
 */
object CounterAccumulator {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val parameters = ParameterTool.fromArgs(args)
    val inputPath = parameters.get("input")
    val outPath = parameters.get("output")
    import org.apache.flink.api.scala._
    val source = env.readTextFile(inputPath)
    source.map(new RichMapFunction[String, String] {
      var counter = new LongCounter()

      override def open(parameters: Configuration): Unit = {
        getRuntimeContext.addAccumulator("my-accumulator",counter)
      }

      override def map(value: String): String = {
        if (value.toLowerCase().contains("flink")){
          counter.add(1)
        }
        value
      }
    }).setParallelism(4).writeAsText(outPath)

    val job = env.execute()
    //获取累加器
    val a = job.getAccumulatorResult[Long]("my-accumulator")
    println(a)
  }
}
