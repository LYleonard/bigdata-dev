package com.wrp.traitE

import java.util.Date

import com.wrp.classMethod.DateUtils

/**
  * @Author LYleonard
  * @Date 2020/5/7 16:04
  * @Description TODO
  */
class ConsoleLogger1 extends Logger1 {
  override def log(msg: String): Unit = println("INFO: " + msg)
}

object ConsoleLogger1 {
  def main(args: Array[String]): Unit = {
    val logger = new ConsoleLogger1
    logger.log("控制台日志：" + DateUtils.format(new Date()))
  }
}
