package com.wrp.pattern

/**
  * @Author LYleonard
  * @Date 2020/5/8 9:06
  * @Description TODO
  */
object CaseList extends App {
  val list = List(1, 3, 6)
  list match {
    case 0::Nil => println("only 0")
    case 0::tail => println("0...")
    case x::y::z::Nil => println(s"x:$x y:$y z:$z")
    case _ => println("something else")
  }
}
