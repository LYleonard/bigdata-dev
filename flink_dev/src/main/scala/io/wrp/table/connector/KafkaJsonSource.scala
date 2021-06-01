package io.wrp.table.connector

import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.types.Row

/**
 * @author DELL
 * @date 2021/3/22 17:48
 * @description 使用flink查询kafka当中的数据
 */
object KafkaJsonSource {
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
//    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(streamEnv)

    import org.apache.flink.api.scala._
    import org.apache.flink.table.api._

    //开启checkpoint
    streamEnv.enableCheckpointing(100)
    streamEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    streamEnv.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    streamEnv.getCheckpointConfig.setCheckpointTimeout(60000)
    streamEnv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    streamEnv.getCheckpointConfig.enableExternalizedCheckpoints(
      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

//    tableEnv.executeSql(
//      """
//        |create table kafka_source_table (
//        |`user_id` int,
//        |`page_id` int,
//        |`status` string
//        |) with (
//        |'connector' = 'kafka',
//        |'topic' = 'kafka_source_table',
//        |'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
//        |'properties.group.id' = 'test_group',
//        |'scan.startup.mode' = 'earliest-offset',
//        |'format' = 'json'
//        |)
//        |""".stripMargin)
//
//    tableEnv.executeSql(
//      """
//        |create table output_kafka(
//        |`user_id` int,
//        |`page_id` int,
//        |`status` string
//        |) with (
//        |'connector' = 'kafka',
//        |'topic' = 'output_kafka',
//        |'properties.bootstrap.servers' = 'hadoop01:9092,hadoop02:9092,hadoop03:9092',
//        |'format' = 'json',
//        |'sink.partitioner' = 'round-robin'
//        |)
//        |""".stripMargin)
//    val sql = "select * from kafka_source_table where status = 'success'"
//
//    val resultTable: Table = tableEnv.sqlQuery(sql)
//    val resultDS: DataStream[(Boolean, UserLog)] = tableEnv.toRetractStream[UserLog](resultTable)
//    resultDS.print()
//    tableEnv.executeSql("insert into output_kafka select * from " + resultTable)
//    streamEnv.execute()
    val kafka: Kafka = new Kafka()
      .version("universal")
      .topic("kafka_source_table")
      .startFromLatest()
      .property("group.id", "test_group")
      .property("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")

    val json: Json = new Json().failOnMissingField(false).deriveSchema()
    //{"userId":1119,"day":"2017-03-02","begintime":1488326400000,"endtime":1488327000000,"data":[{"package":"com.browser","activetime":120000}]}
    val schema: Schema = new Schema()
      .field("userId", DataTypes.INT())
      .field("day", DataTypes.STRING())
      .field("begintime", DataTypes.BIGINT())
      .field("endtime",DataTypes.BIGINT())

    tableEnv
      .connect(kafka)
      .withFormat(json)
      .withSchema(schema)
      .inAppendMode()
      .createTemporaryTable("user_log")
    //使用sql来查询数据
    val table: Table = tableEnv.sqlQuery("select userId,`day`, begintime, endtime from user_log")
    table.printSchema()
    val result: DataStream[(Boolean, UserLog)] = tableEnv.toRetractStream[UserLog](table)
    result.print()
    streamEnv.execute("kafkaSource")

  }
}

case class UserLog(userId: Int, day:String, begintime: Long, endtime: Long)
