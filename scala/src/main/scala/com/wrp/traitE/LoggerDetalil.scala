package com.wrp.traitE

trait LoggerDetalil {
  //在trait中定义具体方法
  def log(msg:String) = println(msg)
}

class PersonService extends LoggerDetalil{
  def add() = log("添加用户")
}

object MethodInTrait{
  def main(args: Array[String]): Unit = {
    val personService = new PersonService
    personService.add()
    personService.log("Method in Trait")
  }
}
