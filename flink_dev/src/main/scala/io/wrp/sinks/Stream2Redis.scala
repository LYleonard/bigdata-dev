package io.wrp.sinks

import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
  * @author LYleonard
  * @date 2021/2/24 9:59
  * @description 自定义sink算子，将数据发送到redis
  */
object Stream2Redis {
  def main(args: Array[String]): Unit = {
    //获取程序入口
    val env: StreamExecutionEnvironment  = StreamExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._
    val streamSource: DataStream[String] = env.fromElements("hello world", "key value")
    //将数据转换成key/value对的tuple类型
    val tupleStream: DataStream[(String, String)] = streamSource.map(x => (x.split(" ")(0), x.split(" ")(1)))

    val builder = new FlinkJedisPoolConfig.Builder
    builder.setHost("slave2")
    builder.setPort(6379)
    builder.setPassword("243015")

    builder.setTimeout(5000)
    builder.setMaxTotal(50)
    builder.setMaxIdle(10)
    builder.setMinIdle(5)
    builder.setDatabase(1)

    val config: FlinkJedisPoolConfig = builder.build()
    //获取redis sink
    val redisSink: RedisSink[(String, String)] = new RedisSink[(String, String)](config, new MyRedisMapper)

    tupleStream.addSink(redisSink)
    env.execute("Redis Sink")
  }
}

class MyRedisMapper extends RedisMapper[(String, String)]{
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.SET)
  }

  override def getKeyFromData(data: (String, String)): String = {
    data._1
  }

  override def getValueFromData(data: (String, String)): String = {
    data._2
  }
}
