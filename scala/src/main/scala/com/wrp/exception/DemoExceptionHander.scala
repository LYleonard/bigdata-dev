package com.wrp.exception

/**
  * @Author LYleonard
  * @Date 2020/5/8 10:14
  * @Description TODO
  */
object DemoExceptionHander {
  def main(args: Array[String]): Unit = {
    try {
      val i = 10/0
    } catch {
      case ex:Exception => println(ex.getMessage)
    } finally {
      println("程序异常与否都会执行的语句！")
    }
  }
}
