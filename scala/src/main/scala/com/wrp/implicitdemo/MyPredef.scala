package com.wrp.implicitdemo

import java.io.File

import scala.io.Source

/**
  * @Author LYleonard
  * @Date 2020/5/8 15:34
  * @Description 隐式转换和隐式参数
  *    所有的隐式转换和隐式参数必须定义在一个object中
  *    案例：让File类具备RichFile类中的read方法
  */
object MyPredef {
  implicit def file2Richfile(file: File): RichFile = new RichFile(file)
}

class RichFile(val file: File){
  //读取数据文件的方法
  def read():String = {
    Source.fromFile(file).mkString
  }
}

object RichFile{
  def main(args: Array[String]): Unit = {
    val file = new File("E:\\develop\\Java\\bigdata-dev\\" +
      "scala\\src\\test\\scala\\com\\wrp\\test.txt")
    //手动导入隐式转换
    import MyPredef.file2Richfile
    val data:String = file.read()
    println(data)
  }
}
