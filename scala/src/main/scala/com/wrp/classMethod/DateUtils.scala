package com.wrp.classMethod

import java.text.SimpleDateFormat
import java.util.Date

/**
  * @Author LYleonard
  * @Date 2020/5/7 11:03
  * @Description 单例对象object，格式化日期时间
  *             scala中是没有Java中的静态成员的。
  *             如果将来我们需要用到static变量、static方法，
  *             就要用到scala中的单例对象object
  */
object DateUtils {
  //在object中定义的成员变量，相当于Java中的一个静态变量
  // 定义一个SimpleDateFormat日期时间格式化对象
  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  // 相当于Java中定义一个静态方法
  def format(date: Date): String = simpleDateFormat.format(date)

  // main是一个静态方法，必须要写在object中
  def main(args: Array[String]): Unit = {
    println(DateUtils.format(new Date()))
  }
}
