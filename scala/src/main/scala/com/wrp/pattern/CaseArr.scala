package com.wrp.pattern

/**
  * @Author LYleonard
  * @Date 2020/5/8 9:02
  * @Description TODO
  */
object CaseArr extends App {
  val arr = Array(1,3,5)

  arr match {
    case Array(1, x, y) => println(x + "---" + y)
    case Array(1, _*) => println("1...")
    case Array(0) => println("only 0")
    case _ => println("something else")
  }
}
