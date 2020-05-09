package com.wrp.covariance

/**
  * @Author LYleonard
  * @Date 2020/5/8 14:55
  * @Description 协变、逆变、非变
  * C[+T]：如果A是B的子类，那么C[A]是C[B]的子类。
  * C[-T]：如果A是B的子类，那么C[B]是C[A]的子类。
  * C[T]： 无论A和B是什么关系，C[A]和C[B]没有从属关系。
  */
class Super
class Sub extends Super

//非变
class Temp1[A](title:String)

//协变
class Temp2[+A](title:String)

//逆变
class Temp3[-A](title:String)

object CovarianceDemo {
  def main(args: Array[String]): Unit = {
    val a = new Sub()
    val b:Super = a

    // 非变
    val t1:Temp1[Sub] = new Temp1[Sub]("Test")
//    val t2:Temp1[Super] = t1 // 报错！默认不允许转换

    //协变
    val t3:Temp2[Sub] = new Temp2[Sub]("Test协变")
    val t4:Temp2[Super] = t3

    //逆变
    val t5:Temp3[Super] = new Temp3[Super]("Test逆变")
    val t6:Temp3[Sub] = t5
  }
}
