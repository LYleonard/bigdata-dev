package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * @Author LYleonard
  * @Date 2020/5/15 9:35
  * @Description SparkSQL DataFrame与DataSet
  */
object SqlTest {
  //样例类
  case class Person(id: String, name: String, age: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("sparksql")
      .master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val person: RDD[Array[String]] = sc.textFile("E:\\tmp\\data\\person.txt")
      .map(_.split(","))

    //样例类与RDD关联
    val personRDD: RDD[Person] = person.map(x => Person(x(0), x(1), x(2).toInt))

    //导入隐式转换
    //启用隐式转换时，需要在 main 函数中自行创建 SparkSession 对象，
    // 然后在对象中导入隐式转换，而非在 object 对象之前导入。
    import spark.implicits._
    //case class 样例类的声明要放在 main 函数前，
    // 否则会报：value toDF is not a member of org.apache.spark.rdd.RDD
    val personDF = personRDD.toDF//.cache()
    personDF.schema
    personDF.show()

    //===============DSL风格语法==================
    personDF.select("name").show()
    personDF.select($"name").show()
    personDF.select(col("name")).show()

    personDF.select($"name", $"age", $"age" + 1).show()

    //过滤
    personDF.filter($"age" > 20)
    //按照age分组统计次数
    personDF.groupBy($"age").count().show()
    //按照age分组统计次数降序
    personDF.groupBy("age").count().sort($"count".desc).show()

    //===============SQL风格语法==================
    personDF.createTempView("person")
    //使用SparkSession调用sql方法，执行sql语句
    spark.sql("select * from person").show()
    spark.sql("select * from person where age > 20").show()
    spark.sql("select age, count(*) from person group by age").show()
    spark.sql("select * from person order by age desc").show()

    //关闭sparkSession对象
    spark.close()
  }
}
