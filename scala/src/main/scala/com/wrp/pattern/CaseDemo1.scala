package com.wrp.pattern

import scala.util.Random

/**
  * @Author LYleonard
  * @Date 2020/5/7 23:58
  * @Description TODO
  */
object CaseDemo1 extends App {
  val arr = Array("hadoop", "zookeeper", "spark", "flink")

  //随机选取
  val name = arr(Random.nextInt(arr.length))
  println(name)

  name match {
    case "hadoop" => println("大数据存储计算框架：" + name)
    case "zookeeper" => println("大数据协调服务框架：" + name)
    case "spark" => println("大数据内存计算框架：" + name)
    case "flink" => println("大数据流计算框架：" + name)
    case _ => println("其他")
  }
}
