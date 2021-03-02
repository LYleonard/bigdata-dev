package io.wrp.batch.parameters

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * @author DELL
 * @date 2021/3/2 11:29
 * @description 通过构造器传递参数
 */
object ConstructorParameter {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val sourceSet: DataSet[String] = env.fromElements("hello flink","abc test")
    sourceSet.filter(new MyFilterFunction("flink")).print()
//    env.execute() //print()方法自动会调用execute()方法，既调print又调env.execute()会报错误
  }
}

class MyFilterFunction(parameter: String) extends FilterFunction[String]{
  override def filter(value: String): Boolean = {
    if(value.contains(parameter)) {
      true
    } else {
      false
    }
  }
}
