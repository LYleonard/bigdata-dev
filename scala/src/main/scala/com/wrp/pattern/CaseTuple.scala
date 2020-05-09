package com.wrp.pattern

/**
  * @Author LYleonard
  * @Date 2020/5/8 9:10
  * @Description TODO
  */
object CaseTuple extends App {
  val tuple = (1,3,5)
  tuple match {
    case (1, x, y) => println(s"1, $x, $y")
    case (2, x, y) => println(s"2, $x, $y")
    case _ => println("others ...")
  }
}
