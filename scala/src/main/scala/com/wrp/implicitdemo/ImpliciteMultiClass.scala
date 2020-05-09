package com.wrp.implicitdemo

/**
  * @Author LYleonard
  * @Date 2020/5/8 16:10
  * @Description TODO
  */

class C

class A(c: C) {
  def readBook(): Unit = {
    println("A said: good book!")
  }
}

class B(c: C) {
  def readBook(): Unit = {
    println("B said: don't understand!")
  }

  def writeBook(): Unit = {
    println("B said: could't write!")
  }
}

object AB {
  //创建一个类型转换为2个类的隐式转换
  implicit def C2A(c:C): A = new A(c)
  implicit def C2B(c:C): B = new B(c)
}

object ImpliciteMultiClass {
  def main(args: Array[String]): Unit = {
    //导包
    //1. import AB._ 会将AB类下的所有隐式转换导进来
    //2. import AB.C2A 只导入C类到A类的的隐式转换方法
    //3. import AB.C2B 只导入C类到B类的的隐式转换方法
    import AB._
    val c = new C

    //由于A类与B类中都有readBook()，只能导入其中一个，否则调用共同方法时代码报错
//    c.readBook()
    c.writeBook()
  }
}
