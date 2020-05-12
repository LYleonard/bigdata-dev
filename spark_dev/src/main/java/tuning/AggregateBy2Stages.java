package tuning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * @ClassName tuning.AggregateBy2Stages
 * @Author LYleonard
 * @Date 2020/5/12 11:56
 * @Description 两阶段聚合（局部聚合+全局聚合）解决数据倾斜问题
 * *      对RDD执行reduceByKey等聚合类shuffle算子或者在Spark SQL中使用group by语句进行分组聚合时，
 * *      比较适用这种方案。 通过为key添加随机数前缀解决。
 * Version 1.0
 **/
public class AggregateBy2Stages {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("AggregateBy2Stages").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        sc.setLogLevel("WARN");

        JavaRDD<String> data = sc.textFile("E:\\downloads\\wordcount.txt");

        //切分每一行获取所有的单词
        JavaRDD<String> words = data.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                String[] words = line.replaceAll("[,.?!:\\-]", "").split(" ");
                return Arrays.asList(words).iterator();
            }
        });

        // 第一步，给RDD中的每个key都打上一个随机前缀。
        JavaPairRDD<String, Integer> randomPrefixRDD = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        Random random = new Random();
                        int prefix = random.nextInt(10);
                        return new Tuple2<String, Integer>(prefix + "_" + s, 1);
                    }
                });

        // 第二步，对打上随机前缀的key进行局部聚合。
        JavaPairRDD<String, Integer> localAggrRdd = randomPrefixRDD.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });

        // 第三步，去除RDD中每个key的随机前缀。
        JavaPairRDD<String, Integer> removedRandomPrefixRdd = localAggrRdd.mapToPair(
                new PairFunction<Tuple2<String, Integer>, String, Integer>() {
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple)
                            throws Exception {
                        String originalKey = tuple._1.split("_")[1];
                        return new Tuple2<String, Integer>(originalKey, tuple._2);
                    }
                }
        );

        //第四步，对去除了随机前缀的RDD进行全局聚合。
        JavaPairRDD<String, Integer> globalAggrRdd = removedRandomPrefixRdd.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });

        List<Tuple2<String, Integer>> results = globalAggrRdd.collect();
        for (Tuple2<String, Integer> result: results){
            System.out.println("word: " + result._1 + "\t次数: " + result._2);
        }
    }
}
