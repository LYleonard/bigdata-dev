package io.wrp.batch

import org.apache.flink.api.scala.ExecutionEnvironment

import scala.collection.mutable.ListBuffer

/**
 * @author DELL
 * @date 2021/3/2 9:43
 * @description 外连接
 */
object BatchOuterJoin {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val product = ListBuffer[(Int, String)]()
    product.append((1, "Flink"))
    product.append((2, "Spark"))
    product.append((3, "Kafka connect"))

    val corporate = ListBuffer[(Int, String)]()
    corporate.append((1, "alibaba"))
    corporate.append((2, "apache"))
    corporate.append((4, "google"))

    val productSet = env.fromCollection(product)
    val corporateSet = env.fromCollection(corporate)

    productSet.leftOuterJoin(corporateSet).where(0).equalTo(0)
      .apply((first, second)=>{
        if (second == null){
          (first._1, first._2, "null")
        } else {
          (first._1, first._2, second._2)
        }
      }).print()
    println("==================================")

    productSet.rightOuterJoin(corporateSet).where(0).equalTo(0)
      .apply((first, second)=>{
        if (first == null){
          (second._1, "null", second._2)
        } else {
          (second._1, first._2, second._2)
        }
      }).print()
    println("==================================")

    productSet.fullOuterJoin(corporateSet).where(0).equalTo(0)
      .apply((first, second)=>{
        if (first == null){
          (second._1, "null", second._2)
        } else if (second == null) {
          (first._1, first._2, "null")
        } else {
          (first._1, first._2, second._2)
        }
      }).print()
    println("==================================")
  }
}
