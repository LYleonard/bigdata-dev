package com.wrp.pattern

import scala.util.Random

case class SubmitTask(id:String, name:String)
case class HeartBeat(time:Long)
case object CheckTimeOutTask

object CaseClassPattern extends App {
  val arr = Array(CheckTimeOutTask, HeartBeat(1000),SubmitTask("0001", "application-task-0001"))

  arr(Random.nextInt(arr.length)) match {
    case SubmitTask(id, name) => println(s"id=$id, name=$name")
    case HeartBeat(time) => println(s"heartbeat time=$time")
    case CheckTimeOutTask => println("check task timeout!")
  }
}
