package io.wrp.sources

import org.apache.flink.streaming.api.functions.source.{ParallelSourceFunction, SourceFunction}

/**
  * @author LYleonard
  * @date 2020-6-21 16:43
  * @description 自定义数据源，继承ParallelSourceFunction
  **/
class MyParallelSource extends ParallelSourceFunction[String]{
  var isRunning: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    while (true) {
      sourceContext.collect("Hello Flink")
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
