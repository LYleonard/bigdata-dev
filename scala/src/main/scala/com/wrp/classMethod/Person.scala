package com.wrp.classMethod

/**
  * @Author LYleonard
  * @Date 2020/5/7 11:36
  * @Description 实现伴生对象的apply方法
  */
class Person(var name: String, var age: Int) {
  override def toString: String = s"Persson($name, $age)"
}

object Person {
  //实现apply方法，返回半生类的对象
  def apply(name: String, age: Int): Person = new Person(name, age)

  // apply方法支持重载
  def apply(name: String): Person = new Person(name, 20)
  def apply(age: Int): Person = new Person("anyone", age)
  def apply(): Person = new Person("anyone", 20)
}

object Main {
  def main(args: Array[String]): Unit = {
    val p1 = Person("zhangsan", 18)
    val p2 = Person("lisi")
    val p3 = Person(28)
    val p4 = Person()

    println(p1)
    println(p2)
    println(p3)
    println(p4)
  }
}