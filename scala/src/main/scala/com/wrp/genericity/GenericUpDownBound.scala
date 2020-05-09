package com.wrp.genericity

//要控制Person只能和Person、Policeman聊天，但是不能和Superman聊天。此时，还需要给泛型添加一个下界。
class PairUpDown[T <: Person, S >: Policeman <: Person](val first: T, val second: S) {
  def chat(msg: String) = println(s"${first.name}向${second.name}说：$msg")
}

case class Person(var name: String, var age: Int)
class Policeman(name: String, age: Int) extends Person(name, age)
class Superman(name: String) extends Policeman(name, -1)

object GenericUpDownBound {
  def main(args: Array[String]): Unit = {
    // 编译错误：第二个参数必须是Person的子类（包括本身）、Policeman的父类（包括本身）
    //    val per = new PairUpDown[Person, Superman](Person("zhangsi", 20), new Superman("wangxiao"))

    val per = new PairUpDown[Person, Policeman](Person("zhangsi", 20),
      new Policeman("wangxiao", 18))
    per.chat("能交流吗？")

    val per2 = new PairUpDown[Person, Person](Person("wangxiao", 18), Person("zhangsi", 20))
    per2.chat("可以交流")
  }
}