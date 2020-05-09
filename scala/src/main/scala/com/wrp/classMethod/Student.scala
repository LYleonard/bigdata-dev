package com.wrp.classMethod

/**
  * @Author LYleonard
  * @Date 2020/5/7 10:55
  * @Description 类构造器
  */
class Student(val name:String, val age:Int) {
  val address:String = "Beijing"

  //定义参数的辅助构造器
  def this(name: String){
    // 第一行必须调用主构造器、其他辅助构造器或者super父类的构造器
    this(name, 20)
  }

  def this(age:Int){
    this("anyone", age)
  }
}
