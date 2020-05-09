package com.wrp.genericity

// 类名后面的方括号，就表示这个类可以使用两个类型、分别是T和S,这两个符号可以任意取
class Pair[T, S](val first: T, val second:S)

case class People(var name:String, var age: Int)

object GenericClass {
  def main(args: Array[String]): Unit = {
    val p1 = new Pair[String, Int]("zhangsi", 18)
    val p2 = new Pair[String, String]("zhangwu", "Beijing")
    val p3 = new Pair[People, People](People("lihua", 22), People("xiaoming", 17))
    println("p1: " + p1.first + "\n" + "p2: " + p2.first + "\n"  + "p3: " + p3.first.name + "," + p3.second.name)
  }
}
