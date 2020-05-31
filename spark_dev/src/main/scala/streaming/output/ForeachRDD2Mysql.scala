package streaming.output

import java.sql.DriverManager

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @Author LYleonard
  * @Date 2020/5/31 20:59
  * @Description foreachRDD输出结果到MySQL
  */
object ForeachRDD2Mysql {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkConf = new SparkConf().setAppName("ForeachRDD2Mysql").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val socketTextStream = ssc.socketTextStream("master", 9999)
    val result = socketTextStream.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_+_)

//    //写法一(错误写法，会报未序列化异常)
//    output2Mysql1(result)
//    //写法二
//    output2Mysql2(result)
//    //写法三
//    output2Mysql3(result)
    //写法四
    output2Mysql4(result)

    ssc.start()
    ssc.awaitTermination()
  }

  def output2Mysql1(result: DStream[(String, Int)]): Unit = {
    result.foreachRDD(rdd => {
      //这里创建的对象都是在Driver端
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test",
        "root", "243015")
      val statement = conn.prepareStatement(s"insert into wordcount(word, count) values(?, ?) " +
        s"on duplicate key update count=values(count)")
      rdd.foreach(record =>{
        statement.setString(1, record._1)
        statement.setInt(2, record._2)
        statement.execute()
      })
      statement.close()
      conn.close()
    })
  }

  /**
    * 由于频繁建立连接所有效率差
    * @param result
    */
  def output2Mysql2(result: DStream[(String, Int)]): Unit = {
    result.foreachRDD(rdd => {
      rdd.foreach(record => {
        val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test",
          "root", "243015")
        val statement = conn.prepareStatement("insert into wordcount(word, count) values (?, ?)")
        statement.setString(1, record._1)
        statement.setInt(2, record._2)
        statement.execute()
        statement.close()
        conn.close()
      })
    })
  }

  /**
    * 按分区进行遍历输出，相对于方法二高效
    * @param result
    */
  def output2Mysql3(result: DStream[(String, Int)]): Unit = {
    result.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test",
          "root", "243015")
        val statement = conn.prepareStatement("insert into wordcount(word, count) values (?, ?)")
        iter.foreach(record => {
          statement.setString(1, record._1)
          statement.setInt(2, record._2)
          statement.execute()
        })
        statement.close()
        conn.close()
      })
    })
  }

  def output2Mysql4(result: DStream[(String, Int)]): Unit = {
    result.foreachRDD(rdd => {
      rdd.foreachPartition(iter => {
        val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test",
          "root", "243015")
        val statement = conn.prepareStatement("insert into wordcount(word, count) values (?, ?)")

        //关闭自动提交，改为批量提交
        conn.setAutoCommit(false)
        iter.foreach(record => {
          statement.setString(1, record._1)
          statement.setInt(2, record._2)
          //添加到一个批次
          statement.addBatch()
        })
        //批量提交该分区所有数据
        statement.executeBatch()
        conn.commit()

        statement.close()
        conn.close()
      })
    })
  }
}
