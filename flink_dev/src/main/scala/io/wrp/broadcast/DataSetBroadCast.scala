package io.wrp.broadcast

import java.util

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration

import scala.collection.mutable

/**
 * @author DELL
 * @date 2021/3/3 15:03
 * @description dataSet当中的广播变量,场景订单关联商品
 */
object DataSetBroadCast {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val parameters = ParameterTool.fromArgs(args)
    val productPath = parameters.get("input_p")
    val orderPath = parameters.get("input_o")

//    val productSet: DataSet[String] = env.readTextFile("D:\\test\\product.csv")
    val productSet: DataSet[String] = env.readTextFile(productPath)
    val productMap = new mutable.HashMap[String, String]()

    val productMapSet = productSet.map(x => {
      val array = x.split(",")
      productMap.put(array(0), x)
      productMap
    })

//    val orderSet: DataSet[String] = env.readTextFile("D:\\test\\order.csv")
    val orderSet: DataSet[String] = env.readTextFile(orderPath)
    val resultSet: DataSet[String] = orderSet.map(new RichMapFunction[String, String] {
      var listData: util.List[Map[String, String]] = null
      var allMap = Map[String, String]()
      override def open(parameters: Configuration): Unit = {
        this.listData = getRuntimeContext.getBroadcastVariable[Map[String, String]]("productBroadCast")
        val listResult: util.Iterator[Map[String, String]] = listData.iterator()
        while (listResult.hasNext) {
          allMap = allMap.++(listResult.next())
        }
      }

      override def map(value: String): String = {
        val str = allMap.getOrElse(value.split(",")(2), "null")
        value + "," + str
      }
    }).withBroadcastSet(productMapSet, "productBroadCast")

    resultSet.print()
  }
}
