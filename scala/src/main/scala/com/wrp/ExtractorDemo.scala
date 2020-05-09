package com.wrp

/**
  * @Author LYleonard
  * @Date 2020/5/8 10:40
  * @Description TODO
  */

class Student{
  var name:String = _
  var age:Int = _
  def this(name:String, age:Int){
    this()
    this.name = name
    this.age = age
  }
}

object Student{
  def apply(name: String, age: Int): Student = new Student(name, age)

  def unapply(arg: Student): Option[(String, Int)] = Some((arg.name, arg.age))
}

object ExtractorDemo {
  def main(args: Array[String]): Unit = {
    val zhangsan = Student("zhangsan", 18)

    zhangsan match {
      case Student(name, age) => println(s"Name: $name, Age: $age")
      case _ => println("未匹配")
    }
  }
}
