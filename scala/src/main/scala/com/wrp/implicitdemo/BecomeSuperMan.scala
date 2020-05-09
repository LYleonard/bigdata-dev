package com.wrp.implicitdemo

/**
  * @Author LYleonard
  * @Date 2020/5/8 15:49
  * @Description 隐式转换案例二:超人变身
  */
class Man(val name: String)

class SuperMan(val name: String) {
  def heat(): Unit = println("SuperMan")
}

object BecomeSuperMan {
  //隐式转换方法
  implicit def man2SuperMan(man: Man): SuperMan = new SuperMan(man.name)

  def main(args: Array[String]): Unit = {
    val hero = new Man("hero")
    hero.heat()
  }
}
