package io.wrp.batch.parameters

import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * @author DELL
 * @date 2021/3/2 12:21
 * @description 使用withParameters传递参数
 */
object WithParameter {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val sourceSet: DataSet[String] = env.fromElements("hello flink","abc test")
    val configuration = new Configuration()
    configuration.setString("parameterKey","flink")
    val filterSet: DataSet[String] = sourceSet.filter(
      new MyFilter).withParameters(configuration)
    filterSet.print()
//    env.execute()  //print()方法自动会调用execute()方法，既调print又调env.execute()会报错误
  }
}
class MyFilter extends RichFilterFunction[String]{
  var value:String ="";
  override def open(parameters: Configuration): Unit = {
    value = parameters.getString("parameterKey","defaultValue")
  }
  override def filter(t: String): Boolean = {
    if(t.contains(value)){
      true
    }else{
      false
    }
  }
}
