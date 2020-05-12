package tuning

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author LYleonard
  * @Date 2020/5/12 14:54
  * @Description 将reduce join转为map join
  *   在对RDD使用join类操作，或者是在Spark SQL中使用join语句时，而且join操作中的一个RDD或表的数据量
  *   比较小（比如几百M或者一两G），比较适用此方案
  */
object MapJoinCase {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("MapSideJoin").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    val bigtb = Array(
      Tuple2("001","stu1"), Tuple2("002","stu2"), Tuple2("003","stu3"),
      Tuple2("004","stu4"), Tuple2("001","stu5"), Tuple2("001","stu6")
    )
    //小表
    val smalltb=Array(Tuple2("001","一班"), Tuple2("002","二班"))

    val bigRDD = sc.parallelize(bigtb)
    val smallRDD = sc.parallelize(smalltb)
//    val result: RDD[(String, (String, String))] = bigRDD.join(smallRDD)
    //设置广播变量
    val broadcastVar = sc.broadcast(smallRDD.collect())
    bigRDD.map(tuple => {
      val id = tuple._1
      val name = tuple._2
      val mapVar = broadcastVar.value.toMap
      val className = mapVar.get(id)
      (id,(name, className))
    }).foreach(tuple => {
      if (tuple._2._2.isDefined){
        println("班级号:"+tuple._1 + ",\t姓名："+tuple._2._1 + ",\t班级名："+tuple._2._2.get)
      }
    })
  }
}
