import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Array;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @ClassName JavaWordCount
 * @Author LYleonard
 * @Date 2020/5/9 11:02
 * @Description TODO
 * Version 1.0
 **/
public class JavaWordCount {
    public static void main(String[] args) {
        // 创建SparkConf对象
        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount").setMaster("local[*]");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> data = sparkContext.textFile("E:\\downloads\\wordcount.txt");

        //切分每一行获取所有的单词
        JavaRDD<String> words = data.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String line) throws Exception {
                String[] words = line.replaceAll("[,.?!:\\-]", "").split(" ");
                return Arrays.asList(words).iterator();
            }
        });

        //每个单词计为1
        JavaPairRDD<String, Integer> wordmap = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        //相同单词出现的1累加
        JavaPairRDD<String, Integer> result = wordmap.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<Integer, String> revereResult = result.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<Integer, String>(t._2, t._1);
            }
        });

        JavaPairRDD<Integer, String> sortedRDD = revereResult.sortByKey(false);
        List<Tuple2<Integer, String>> finalResult = sortedRDD.collect();

        for (Tuple2<Integer, String> t : finalResult) {
            System.out.println("word: " + t._2 + "\t次数: " + t._1);
        }

        sparkContext.close();
    }
}
