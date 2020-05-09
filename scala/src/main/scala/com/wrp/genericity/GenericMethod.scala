package com.wrp.genericity

/**
  * @Author LYleonard
  * @Date 2020/5/8 10:52
  * @Description TODO
  */
object GenericMethod {
  //不考虑泛型的支持
  def getMiddle(arr:Array[Int]) = arr(arr.length / 2)

  //考虑泛型的支持
  def getMiddle1[A](arr:Array[A]) = arr(arr.length/2)

  def main(args: Array[String]): Unit = {
    val array = Array(1,2,3,4,5,6)
    println("Middle: " + getMiddle(array))


    val arr1 = Array(1,2,3,4,5,6,7,7,9)
    val arr2 = Array("hadoop", "spark", "redis", "Flink")
    println("Middle: " + getMiddle1[Int](arr1))
    println("Middle: " + getMiddle1[String](arr2))
    //简写
    println("Middle: " + getMiddle1(arr1))
    println("Middle: " + getMiddle1(arr2))
  }
}

