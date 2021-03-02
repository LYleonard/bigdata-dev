package io.wrp.batch.partition

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * @author DELL
 * @date 2021/3/2 11:01
 * @description 自定义分区
 */
object CustomerPartition {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    import org.apache.flink.api.scala._

    val sourceDataSet = env.fromElements("hello world","spark flink","hello flink","hive hadoop")
    val result = sourceDataSet.partitionCustom(new MyPartitioner2, x=>x+"")
    val value = result.map(x=>{
      println("数据的key为:" + x + ", 线程号为:" + Thread.currentThread().getId)
      x
    })
    value.print()
    env.execute()
  }
}

class MyPartitioner2 extends Partitioner[String]{
  override def partition(key: String, numPartitions: Int): Int = {
    println("分区个数为" +  numPartitions)
    if(key.contains("hello")){
      println("分区号为：0")
      0
    } else {
      println("分区号为：1")
      1
    }
  }
}
