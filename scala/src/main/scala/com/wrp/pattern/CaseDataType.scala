package com.wrp.pattern

import scala.util.Random

/**
  * @Author LYleonard
  * @Date 2020/5/8 8:51
  * @Description TODO
  */
object CaseDataType extends App {
  val arr = Array("hello", 1, -2, 0, CaseDemo1)
  val value = arr(Random.nextInt(arr.length))
  println("随机取得的数据元素：" + value)

  value match {
    case x:Int => println("Int => " + x)
    case y:Double => println("Double => " + y)
    case z:String => println("String => " + z)
    case _ => throw new Exception("not match exception!")
  }
}
