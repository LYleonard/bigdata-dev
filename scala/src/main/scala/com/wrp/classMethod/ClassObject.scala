package com.wrp.classMethod

/**
  * @Author LYleonard
  * @Date 2020/5/7 11:16
  * @Description 伴生类、伴生对象
  *              (1). 伴生类和伴生对象的名字必须是一样的
  *              (2). 伴生类和伴生对象需要在一个scala源文件中
  *              (3). 伴生类和伴生对象可以互相访问private的属性
  */
class ClassObject {
  val id = 1
  private var name = "scala"

  def printName(): Unit = {
    //在此类中可以访问伴生对象的私有方法
    println(ClassObject.CONTANT + "\t" + name)
  }
}

object ClassObject{
  //伴生对象中的私有属性
  private val CONTANT = "伴生对象"

  def main(args: Array[String]): Unit = {
    val p = new ClassObject
    //访问私有变量
    p.name = "scala object"
    p.printName()
  }
}
