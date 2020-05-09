package com.wrp.traitE

import java.text.SimpleDateFormat
import java.util.Date

trait LoggerEX {
  // 具体字段
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val INFO = "INFO: " + sdf.format(new Date())
  //抽象字段
  val TYPE:String

  //抽象方法
  def log(msg:String)
}

class ConsoleLoggerEx extends LoggerEX {
  override val TYPE: String = "Console"

  override def log(msg: String): Unit =  println(s"$TYPE $INFO $msg")
}

object FieldInTrait {
  def main(args: Array[String]): Unit = {
    val logger = new ConsoleLoggerEx

    logger.log("this is a message to ooutput")
  }
}
