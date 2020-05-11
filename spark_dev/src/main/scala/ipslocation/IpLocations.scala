package ipslocation

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author LYleonard
  * @Date 2020/5/11 14:46
  * @Description 通过spark来实现ip的归属地查询
  */
object IpLocations {

  //把ip地址转换成Long类型数字   192.168.200.100
  def ip2Long(ip: String): Long = {
    val ips: Array[String] = ip.split("\\.")
    var ipNum: Long = 0L

    for (i <- ips){
      ipNum = i.toLong | ipNum << 8L
    }
    ipNum
  }

  //利用二分查询，查询到数字在数组中的下标
  def binarySearch(ipNum: Long, city_ip_array: Array[(String, String, String, String)]): Int = {
    //定义数组的开始下标
    var start = 0

    //定义数组结束下标
    var end = city_ip_array.length-1

    while (start <= end){
      //获取中间下标
      val middle = (start + end)/2
      if (ipNum >= city_ip_array(middle)._1.toLong && ipNum <= city_ip_array(middle)._2.toLong){
        return middle
      }
      if (ipNum < city_ip_array(middle)._1.toLong) {
        end = middle - 1
      }

      if (ipNum > city_ip_array(middle)._2.toLong) {
        start = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("IpLocations").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    //加载城市ip段信息数据，获取(ip的开始数字、ip的结束数字、经度、纬度)
    val city_ip_rdd: RDD[(String, String, String, String)] = sc.textFile("E:\\tmp\\data\\ip.txt")
      .map(_.split("\\|")).map(x => (x(2), x(3), x(x.length-2), x(x.length-1)))

    //使用spark的广播变量把共同的数据广播到参与计算的worker节点的executor进程中
    val cityIpBroadcast: Broadcast[Array[(String, String, String, String)]] = sc.broadcast(city_ip_rdd.collect())

    //读取运营商数据
    val userIpsRDD: RDD[String] = sc.textFile("E:\\tmp\\data\\20090121000132.394251.http.format")
      .map(_.split("\\|")(1))

    //遍历userIpsRDD获取每一个ip地址，然后转换成数字，去广播变量中进行匹配
    val resultRDD: RDD[((String, String), Int)] = userIpsRDD.mapPartitions(iter => {
      //获取广播变量的值
      val city_ip_array: Array[(String, String, String, String)] = cityIpBroadcast.value

      // 获取每个IP地址
      iter.map(ip => {
        //把ip地址转换成数字
        val ipNum: Long = ip2Long(ip)

        //需要把ip转换成Long类型的数子去广播变量中的数组进行匹配，获取long类型的数字在数组中的下标
        val index: Int = binarySearch(ipNum, city_ip_array)

        //获取对应下标的信息
        val result: (String, String, String, String) = city_ip_array(index)

        //封装结果数据，返回（(经度，维度), 1）
        ((result._3, result._4), 1)
      })
    })

    //相同经纬度reduce
    val finalResult = resultRDD.reduceByKey(_ + _)
    //打印输出
    finalResult.foreach(println)

    val sortRDD = finalResult.coalesce(1)
      .sortBy(_._2, ascending = false).foreach(println)

    println("-------------------------")

    val sortedRDD2: Unit = finalResult.mapPartitionsWithIndex((i, iter) => {
      iter.map(x => (x._1._1, x._1._2, x._2, i))
    }).sortBy(_._3, ascending = false, numPartitions = 1).foreach(println)

    sc.stop()
  }
}
