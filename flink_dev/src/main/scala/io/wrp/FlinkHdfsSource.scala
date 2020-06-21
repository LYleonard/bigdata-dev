package io.wrp

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * @author LYleonard
  * @date 2020-6-21 15:45
  * @description TODO
  **/
object FlinkHdfsSource {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val hdfsSource = env.readTextFile("hdfs://hdp2.cetc.bigdata:8020/user/test.txt")

    import org.apache.flink.api.scala._
    val result = hdfsSource.flatMap(_.split(";")).map((_, 1)).keyBy(0).sum(1)

    result.print().setParallelism(1)

    env.execute("HDFS source")
  }
}
