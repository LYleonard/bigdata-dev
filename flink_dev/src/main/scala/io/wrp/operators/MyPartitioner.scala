package io.wrp.operators

import org.apache.flink.api.common.functions.Partitioner

/**
  * @author LYleonard
  * @date 2020/7/6 22:18
  * @description 自定义分区策略，实现不同分区的数据发送到不同分区中处理
  */
class MyPartitioner extends Partitioner[String]{
  override def partition(key: String, numPartitions: Int): Int = {
    println("分区个数为：" + numPartitions)
    if(key.contains("hello")){
      0
    } else {
      1
    }
  }
}
