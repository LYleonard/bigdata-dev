package com.wrp.traitE

import java.util.Date

import com.wrp.classMethod.DateUtils

/**
  * @Author LYleonard
  * @Date 2020/5/7 16:17
  * @Description TODO
  */
class ConsoleLogger2 extends Logger2 with MessageSender {
  override def log(msg: String): Unit = println(msg)

  override def send(msg: String): Unit = println(s"Send message: $msg")
}

object ConsoleLogger2 {
  def main(args: Array[String]): Unit = {
    val logger = new ConsoleLogger2
    logger.log("INFO Console Log")
    logger.send(DateUtils.format(new Date()))
  }
}