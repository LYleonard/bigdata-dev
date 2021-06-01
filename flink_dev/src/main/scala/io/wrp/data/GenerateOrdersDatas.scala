package io.wrp.data

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Date, UUID}

import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.types.Row

import scala.util.Random

/**
 * @author DELL
 * @date 2021/6/1 14:24
 * @description flink的stream流式程序，持续的生成订单数据
 */
object GenerateOrdersDatas {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val sink = JDBCAppendTableSink.builder().setBatchSize(2)
      .setDBUrl("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf-8&serverTimezone=Asia/Shanghai&useSSL=false")
      .setDrivername("com.mysql.cj.jdbc.Driver")
      .setUsername("root")
      .setPassword("ictbda@2018")
      .setQuery("insert into goods(product_id,product_name,type,price,manufacturers) values(?,?,?,?,?)")
      .setParameterTypes(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
        BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.DOUBLE_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
      .build()

    val sourceStream: DataStreamSource[Row] = env.addSource(
      new RichParallelSourceFunction[Row] {
        var isRunning = true
        override def run(ctx: SourceFunction.SourceContext[Row]): Unit = {
          while (isRunning) {
            val order = generateOrder
            ctx.collect(Row.of(order.orderId, order.userId, order.goodsId, order.price,
              order.totalFee, order.payFrom, order.province))
            Thread.sleep(1000)
          }
        }

        override def cancel(): Unit = {
          isRunning = false
        }
    })

    sink.emitDataStream(sourceStream)
    env.execute()
  }

  def generateOrder: Order = {
    val province: Array[String] = Array[String]("北京市", "天津市", "上海市", "重庆市", "河北省",
      "山西省", "辽宁省", "吉林省", "黑龙江省", "江苏省", "浙江省", "安徽省", "福建省", "江西省", "山东省",
      "河南省", "湖北省", "湖南省", "广东省", "海南省", "四川省", "贵州省", "云南省", "陕西省", "甘肃省",
      "青海省")

    val random = new Random()
    val orderId: String = UUID.randomUUID().toString
    val userId: String = UUID.randomUUID().toString
    val goodsId: String = UUID.randomUUID().toString
    val price: Double = (100 + random.nextDouble()*100)
    val price1 = formatDecimal(price,2)
    val totalFee: Double = 150+random.nextDouble()*100
    val totalFee1 = formatDecimal(totalFee,2)
    val payFrom = random.nextInt(5).toString
    val provinceName: String = province(random.nextInt(province.length))
    val date = new Date
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dateStr: String = format.format(date)

    Order(orderId, userId, goodsId, price1, totalFee1, payFrom, provinceName)
  }

  def formatDecimal(d: Double, newScale: Int): String = {
    var pattern = "#."
    var i = 0
    while ( {
      i < newScale
    }) {
      pattern += "#"

      {
        i += 1; i - 1
      }
    }
    val df = new DecimalFormat(pattern)
    df.format(d)
  }


}

case class Order(orderId: String, userId: String, goodsId: String, price: String, totalFee: String,
                 payFrom: String, province: String) extends Serializable
