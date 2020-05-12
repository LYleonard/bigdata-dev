package serializable

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author LYleonard
  * @Date 2020/5/12 21:07
  * @Description TODO
  */

class Address extends Serializable{
  val name = "Beijing"
  //如果成员变量本身不支持序列化，需要添加注解@transient，表示对象不需要序列化
  @transient
  val connection = DriverManager.getConnection("jdbc:mysql://192.168.29.2:3306/userdb", "root", "243015")
}

object SparkTaskSerializable {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkTaskSerializable").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(1 to 10)

    //由于address这个对象在driver端，后期会发送到executor端，因此需要序列化
    val address = new Address
    val result = rdd.map(x => {
      //函数体中依赖于外部类的成员变量
      (x, address.name)
    })

    result.foreach(println)

    sc.stop()
  }
}
