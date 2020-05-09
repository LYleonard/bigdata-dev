package com.wrp.classMethod

/**
  * @Author LYleonard
  * @Date 2020/5/7 15:16
  * @Description 访问修饰符
  */
class Person6 {
//  private[this] var name = "parent class"
  protected [this] var name = "parent class"

  def getName : String = this.name

  def sayHello(p:Person6) = {
    println("Hello" + p.getName)
  }
}

object Person6 {
  def main(args: Array[String]): Unit = {
    val p = new Person6
    println(p.getName)
  }
}

object Student7 extends Person6 {
  def main(args: Array[String]): Unit = {
//    name = name + " Student"
    println(name)
  }
}

class Person8(var name:String) {
  println("name: " + name)
}

class Student8(name: String, var adress:String) extends Person8(name)

object Main8 {
  def main(args: Array[String]): Unit = {
    val s1 = new Student8("litian", "Beijing")
    println(s"${s1.name} - ${s1.adress}")
  }
}

//=================
abstract class Person9(val name:String){
  //抽象方法
  def sayHello:String
  def sayBye:String

  //抽象字段
  def address:String
}

class Student9(name:String) extends Person9(name){
  override def sayHello: String = s"Hello, $name"

  override def sayBye: String = s"Bye, $name"

  override def address: String = "Beijing"
}

object Main9{
  def main(args: Array[String]): Unit = {
    val s = new Student9("Tom")
    println(s.sayHello)
    println(s.address)
    println(s.sayBye)
  }
}

//==============
abstract class Person10{
  def sayHello() : Unit
}

object Main10{
  def main(args: Array[String]): Unit = {
    // 直接用new来创建一个匿名内部类对象
    val p1 = new Person10 {
      override def sayHello(): Unit = println("这是一个匿名内部类！")
    }
    p1.sayHello()
  }
}
