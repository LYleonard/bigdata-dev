package io.wrp.tuning;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName MapSideJoin
 * @Author LYleonard
 * @Date 2020/5/12 15:43
 * @Description 将reduce join转为map join
 * Version 1.0
 **/
public class MapSideJoin {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("MapSideJoin").setMaster("local[*]");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // 小表
        final JavaRDD<String> account = sparkContext.textFile("E:\\downloads\\accounts.csv");
        final JavaPairRDD<Long, String> accountRDD = account.mapToPair(new PairFunction<String, Long, String>() {
            public Tuple2<Long, String> call(String s) throws Exception {
                Long id = (long) Integer.parseInt(s.split(",")[0].replaceAll("\\\"",""));
                return new Tuple2<Long, String>(id, s);
            }
        });
        // 大表
        JavaRDD<String> order = sparkContext.textFile("E:\\downloads\\orders.csv");
        JavaPairRDD<Long, String> orderRDD = order.mapToPair(new PairFunction<String, Long, String>() {
            public Tuple2<Long, String> call(String s) throws Exception {
                Long accountId = (long) Integer.parseInt(s.split(",")[1].replaceAll("\\\"", ""));
                return new Tuple2<Long, String>(accountId, s);
            }
        });

        // 1. 首先将数据量比较小的RDD的数据，collect到Driver中
        List<Tuple2<Long, String>> accountCollect = accountRDD.collect();

        // 2. 使用Spark的广播功能，将小RDD的数据转换成广播变量，这样每个Executor就只有一份RDD的数据。
        // 可以尽可能节省内存空间，并且减少网络传输性能开销。
        final Broadcast<List<Tuple2<Long, String>>> accountBroadcast = sparkContext.broadcast(accountCollect);

        //对数据量大的RDD执行map操作，而不是join操作
        JavaPairRDD<String, Tuple2<String, String>> accountJoinOrder = orderRDD.mapToPair(
                new PairFunction<Tuple2<Long, String>, String, Tuple2<String, String>>() {
                    public Tuple2<String, Tuple2<String, String>> call(Tuple2<Long, String> tuple)
                            throws Exception {
                        // 在算子函数中，通过广播变量，获取到本地Executor中的小数据量的RDD数据
                        List<Tuple2<Long, String>> accountRDD1 = accountBroadcast.value();

                        //将accountRDD1中的数据转换为一个Map，便于后面进行join操作
                        Map<Long, String> accountRDD1Map = new HashMap<Long, String>();
                        for (Tuple2<Long, String> accountData : accountRDD1) {
                            accountRDD1Map.put(accountData._1, accountData._2);
                        }

                        // 获取当前RDD数据的key以及value
                        Long key = tuple._1;
                        String value = tuple._2;
                        // 从account数据Map中，根据key获取到可以join到的数据
                        String accountValue = accountRDD1Map.get(key);
                        return new Tuple2<String, Tuple2<String, String>>(key.toString(),
                                new Tuple2<String, String>(accountValue, value));
                    }
                });
        // 上面的做法，仅仅适用于rdd1中的key没有重复，全部是唯一的场景。
        // 如果rdd1中有多个相同的key，那么就得用flatMap类的操作，
        // 在进行join的时候不能用map，而是得遍历rdd1所有数据进行join。
        // rdd2中每条数据都可能会返回多条join后的数据。
        List<Tuple2<String, Tuple2<String, String>>> results = accountJoinOrder.collect();
        for (Tuple2<String, Tuple2<String, String>> result : results) {
            System.out.println("result: " + result);
        }
    }
}
