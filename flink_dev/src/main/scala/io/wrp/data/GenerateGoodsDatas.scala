package io.wrp.data
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.types.Row

/**
 * @author DELL
 * @date 2021/6/1 11:23
 * @description TODO
 */
object GenerateGoodsDatas {
  def main(args: Array[String]): Unit = {
    val environment = ExecutionEnvironment.getExecutionEnvironment

    import org.apache.flink.api.scala._

    val fileSource = environment.readTextFile("D:\\tmp\\goods.csv")
    val line = fileSource.flatMap(x=>{x.split("\r\n")})

    val productSet = line.map(x=>{
      val pro = x.split(",")
      println(pro(1))
      Row.of(pro(0), pro(1), pro(2), pro(3), pro(4))
    })

    productSet.output(JDBCOutputFormat.buildJDBCOutputFormat()
      .setBatchInterval(2)
      .setDBUrl("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai&useSSL=false")
      .setDrivername("com.mysql.cj.jdbc.Driver").setUsername("root")
      .setPassword("ictbda@2018")
      .setQuery("insert into goods(product_id,product_name,type,price,manufacturers) values(?,?,?,?,?)")
      .finish())

    environment.execute()
  }
}
