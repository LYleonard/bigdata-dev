package io.wrp

import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * @author DELL
 * @date 2021/3/3 17:21
 * @description Flink分布式缓存
 */
object FlinkDistributedCache {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    env.registerCachedFile("D:\\test\\test1.txt", "cache")

    val source = env.fromElements("spark,flink,kafka")
    val result = source.map(new RichMapFunction[String,String] {
      override def open(parameters: Configuration): Unit = {
        super.open(parameters)
        val cacheFile = getRuntimeContext.getDistributedCache.getFile("cache")
        val lines = FileUtils.readLines(cacheFile, "utf-8")
        val iter = lines.iterator()
        while (iter.hasNext) {
          val line = iter.next()
          println("line:" + line)
        }
      }
      override def map(value: String): String = {
        value
      }
    }).setParallelism(1)
    result.print()
  }
}
