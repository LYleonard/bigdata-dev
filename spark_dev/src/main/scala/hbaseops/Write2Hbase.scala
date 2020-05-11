package hbaseops

import java.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author LYleonard
  * @Date 2020/5/11 10:06
  * @Description foreachPartition算子实现把rdd结果数据写入到hbase表
  */
object Write2Hbase {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Write2Hbase").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val usersRDD = sc.textFile("E:\\tmp\\data\\users.dat").map(_.split("::"))

    usersRDD.foreachPartition(iter => {
      var connection: Connection = null
      try {
        val config:Configuration = HBaseConfiguration.create()
        config.set("hbase.zookeeper.quorum", "master:2181,slave1:2181,slave2:2181")
        connection = ConnectionFactory.createConnection(config)

        val table: Table = connection.getTable(TableName.valueOf("person"))
        //把数据插入到hbase表中, hbase shell : put "person",'rowkey','列族：字段'，'value'
        //create 'person','f1','f2'
        iter.foreach(line => {
          val put = new Put(line(0).getBytes())
          val puts = new util.ArrayList[Put]()

          //构建数据
          val put1 = put.addColumn("f1".getBytes, "gender".getBytes, line(1).getBytes)
          val put2 = put.addColumn("f1".getBytes, "age".getBytes, line(2).getBytes)
          val put3 = put.addColumn("f2".getBytes, "position".getBytes, line(3).getBytes)
          val put4 = put.addColumn("f2".getBytes, "code".getBytes, line(4).getBytes)

          puts.add(put1)
          puts.add(put2)
          puts.add(put3)
          puts.add(put4)

          //提交这些put数据
          table.put(puts)
        })

      } catch {
        case e: Exception => println(e.getMessage)
      } finally {
        if (connection != null){
          connection.close()
        }
      }
    })
  }
}
