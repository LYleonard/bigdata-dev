package com.wrp.genericity


class PairUp[T<:People, S<:People](val first:T, val second:S){
  def chat(msg: String) = println(s"${first.name}向${second.name}说：$msg")
}

object GenericUpBound {
  def main(args: Array[String]): Unit = {
    val people = new PairUp[People, People](People("zhangsan", 18), People("lihua", 19))
    people.chat("let's go Lugu lake!")
  }
}
