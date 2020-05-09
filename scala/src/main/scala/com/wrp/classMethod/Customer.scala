package com.wrp.classMethod

import java.util.Date

/**
  * @Author LYleonard
  * @Date 2020/5/7 10:42
  * @Description 类定义, 伴生对象
  */
class Customer {
  var name:String = _
  var sex:String = _
  val registDate:Date = new Date

  def sayHello(msg:String): Unit = {
    println(msg)
  }
}

object Customer{
  def main(args: Array[String]): Unit = {
    val customer = new Customer
    //assignment
    customer.name = "zhangsan"
    customer.sex = "man"
    println(s"Name: ${customer.name}, Sex: ${customer.sex}, RegistDate: ${customer.registDate}")

    customer.sayHello(s"Hello, ${customer.name}")
  }
}
