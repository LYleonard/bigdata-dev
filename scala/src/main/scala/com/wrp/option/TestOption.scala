package com.wrp.option

/**
  * @Author LYleonard
  * @Date 2020/5/8 9:47
  * @Description Option类型用样例类来表示可能存在或也可能不存在的值
  */
object TestOption {
  def main(args: Array[String]): Unit = {
    val map = Map("a" -> 1, "b" -> 2)

    val value:Option[Int] = map.get("b")
    val v1 = value match {
      case Some(x) => x
      case None => -1
    }
    println(v1)

    //更好的方式
    val v2 = map.getOrElse("c", 0)
    println(v2)
  }
}
