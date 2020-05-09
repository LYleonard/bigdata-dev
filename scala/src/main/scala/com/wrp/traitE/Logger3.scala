package com.wrp.traitE

trait Logger3 {
  def log(msg:String)
  def info(msg:String) = log("INFO: " + msg)
  def warn(msg:String) = log("WARN: " + msg)
  def error(msg:String) = log("ERROR: " + msg)
}

class ConsoleLogger3 extends Logger3{
  override def log(msg: String): Unit = println("abstract method in trait implement: " + msg)
}

object LoggerTrait3 {
  def main(args: Array[String]): Unit = {
    val logger3 = new ConsoleLogger3
//    logger3.log("test")
    logger3.info("普通输出")
    logger3.warn("警告信息")
    logger3.error("错误信息")
  }
}