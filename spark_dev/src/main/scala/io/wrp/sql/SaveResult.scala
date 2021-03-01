package sql

import org.apache.spark.sql.SparkSession

/**
  * @Author LYleonard
  * @Date 2020/5/15 23:51
  * @Description 保存数据的不同方法
  */
object SaveResult {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SaveData")
      .master("local[*]").getOrCreate()
    val jsonDF = spark.read.json("E:\\tmp\\data\\score.json")
    jsonDF.createTempView("t_score")

    val result = spark.sql("select * from t_score where score > 80")

    result.select("name").write.text("E:\\tmp\\out\\text")

    result.write.json("E:\\tmp\\out\\json")

    result.write.parquet("E:\\tmp\\out\\parquet")

    result.write.save("E:\\tmp\\\\out\\save")//save方法默认存储格式为parquet

    result.write.csv("E:\\tmp\\out\\csv")

    result.write.saveAsTable("t_score1")

    //按照单个字段分区，分目录存储
    result.write.partitionBy("classNum")
      .json("E:\\tmp\\out\\partitions")

    result.write.partitionBy("classNum", "name")
      .json("E:\\tmp\\out\\numPartitions")

    spark.close()
  }
}
