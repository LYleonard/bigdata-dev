package io.wrp.batch.parameters

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

/**
 * @author DELL
 * @date 2021/3/2 12:26
 * @description 全局参数传递
 */
object GlobalJobParameters {
  def main(args: Array[String]): Unit = {
    val configuration = new Configuration()
    configuration.setString("parameterKey","flink")

    val env=ExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(configuration)
    import org.apache.flink.api.scala._
    val sourceSet: DataSet[String] = env.fromElements("hello flink","abc test")

    val filterSet: DataSet[String] = sourceSet.filter(new MyFilter)
    filterSet.print()
//    env.execute()   //print()方法自动会调用execute()方法，既调print又调env.execute()会报错误
  }
}
class MyFilter1 extends RichFilterFunction[String]{
  var value:String ="";
  override def open(parameters: Configuration): Unit = {
    val parameters: ExecutionConfig.GlobalJobParameters = getRuntimeContext.getExecutionConfig.getGlobalJobParameters
    val globalConf:Configuration =  parameters.asInstanceOf[Configuration]
    value = globalConf.getString("parameterKey","test")
  }
  override def filter(t: String): Boolean = {
    if(t.contains(value)){
      true
    }else{
      false
    }
  }
}

