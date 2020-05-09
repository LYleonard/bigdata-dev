package com.wrp.traitE

trait LoggerMix {
  def log(msg:String) = println(msg)
}
class UserService

object FixedInClass {
  def main(args: Array[String]): Unit = {
    //使用with关键字直接将特质混入到对象中，是对象具有特质中定义的方法（行为）
    val userService = new UserService with LoggerMix

    userService.log("用户你好！")
  }
}