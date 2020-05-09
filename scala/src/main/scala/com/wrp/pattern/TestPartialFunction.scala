package com.wrp.pattern

/**
  * @Author LYleonard
  * @Date 2020/5/8 10:00
  * @Description TODO
  */
object TestPartialFunction {
  val fun1: PartialFunction[Int, String] = {
    case 1 => "String 1"
    case 2 => "String 2"
    case 3 => "String 3"
    case _ => "others"
  }

  def main(args: Array[String]): Unit = {
    println(fun1(2))

    val list = List(1,2,3,4,5,6)
    //使用偏函数处理
    val result = list.filter{
      case x if x % 2 ==0 => true
      case _ => false
    }
    println(result)
  }
}
