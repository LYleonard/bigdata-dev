package com.wrp.classMethod

/**
  * @Author LYleonard
  * @Date 2020/5/7 11:57
  * @Description 继承
  */
class Person1 {
  var name = "super"

  def getName : String = this.name
}

class Student1 extends Person1

object Main1{
  def main(args: Array[String]): Unit = {
    val p1 = new Person1()
    val st = new Student1()

    st.name = "Tom"
    println("Name: " + st.getName)
  }
}
