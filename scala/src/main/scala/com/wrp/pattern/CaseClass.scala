package com.wrp.pattern

/**
  * @Author LYleonard
  * @Date 2020/5/8 9:17
  * @Description TODO
  */

//样例类有两个成员name、age
case class CaseClass(name:String, age:Int)

//使用var指定成员变量可变
case class CaseStudent(var name:String, var age:Int)

object CaseClassObj {
  def main(args: Array[String]): Unit = {
    val person = new CaseClass("zhangsan", 18)
    println(person)

    //使用类名直接创建实例
    val person2 = CaseClass("lisi", 19)
    println(person2)

    // 3. 样例类默认的成员变量都是val的，除非手动指定变量为var类型
    //person2.age = 22  // 编译错误！age默认为val类型
    val student = CaseStudent("xiaoming", 17)
    student.age = 23
    println(student)
  }
}