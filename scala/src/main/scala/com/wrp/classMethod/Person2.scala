package com.wrp.classMethod

/**
  * @Author LYleonard
  * @Date 2020/5/7 14:03
  * @Description TODO
  */
class Person2 {
  val name = "super"
  def getName : String = name
}

object Student2 extends Person2

object Main2 {
  def main(args: Array[String]): Unit = {
    println(Student2.getName)
  }
}

//=============================
class Student3 extends Person2 {
  //重写val变量
  override val name:String = "child"

  // 重写getName方法
  override def getName: String = "Hello, " + super.getName
}

object Main3{
  def main(args: Array[String]): Unit = {
    println(new Student3().getName)
  }
}

class Person4
class Student4 extends Person4
object Main4 {
  def main(args: Array[String]): Unit = {
    val s1:Person4 = new Student4
    //isInstanceOf 只能判断出对象是否为指定类以及其子类的对象
    // 判断s1是否为Student4类型
    if (s1.isInstanceOf[Student4]){
      //s1转换为Student3类型
      val s2 = s1.asInstanceOf[Person4]
      println("s2类型: " + s2)
    }
  }
}

//==================
class Person5
class Student5 extends Person5

object Student5 {
  def main(args: Array[String]): Unit = {
    val p: Person5 = new Student5

    //判断p的类型时是否为Person5
    println(p.isInstanceOf[Person5])

    //判断p的类型是否为Person5类
    println(p.getClass == classOf[Person5])

    //判断p的类型是否为Student5类
    println(p.getClass == classOf[Student5])
  }
}
