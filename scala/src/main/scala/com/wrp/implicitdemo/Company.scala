package com.wrp.implicitdemo

/**
  * @Author LYleonard
  * @Date 2020/5/8 16:25
  * @Description 隐式参数案例四:员工领取薪水
  */
object Company {
  //在object中定义隐式值    注意：同一类型的隐式值只允许出现一次，否则会报错
  implicit val xxx = "zhangsan"
  implicit val yyy = 10000.00
  implicit val zzz = "lisi"
}

class Boss{
  //定义一个用implicit修饰的参数 类型为String
  //注意参数匹配的类型   它需要的是String类型的隐式值
  def callName(implicit name: String): String = {
    name + " is comming!"
  }

  //定义一个用implicit修饰的参数，类型为Double
  //注意参数匹配的类型    它需要的是Double类型的隐式值
  def salary(implicit money: Double): String = {
    "salary is: " + money
  }
}

object Boss extends App{
  //使用import导入定义好的隐式值，注意：必须先加载否则会报错
  import Company.xxx
  import Company.yyy
//  import Company.zzz //同时导入同一类型的隐式参数会报错

  val boss = new Boss
  println(boss.callName + boss.salary)
}