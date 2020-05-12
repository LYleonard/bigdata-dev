package wordcount

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @Author LYleonard
  * @Date 2020/5/12 14:39
  * @Description 如果使用reduceByKey因为数据倾斜造成运行失败的问题。具体操作流程如下:
  *              (1) 将原始的 key 转化为  随机值 + key  (随机值 = Random.nextInt)
  *              (2) 对数据进行 reduceByKey(func)
  *              (3) 将  随机值+key 转成 key
  *              (4) 再对数据进行 reduceByKey(func)
  */
object WordCountAggr2Stages {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("E:\\downloads\\wordcount.txt")
    rdd.flatMap( line => line.replaceAll("[,.!?\\-]", "").split(" "))
      .map(word =>{
        val prefix = (new util.Random).nextInt(3)
        (prefix+"_"+word,1)
      }).reduceByKey(_+_)
      .map( wc =>{
        val newWord=wc._1.split("_")(1)
        val count=wc._2
        (newWord,count)
      }).reduceByKey(_+_)
      .foreach( wc =>{
        println("单词："+wc._1 + " 次数："+wc._2)
      })
  }
}
