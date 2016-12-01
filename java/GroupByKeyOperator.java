import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * groupByKey算子
 * reduce端根据key聚合的算子，尽量使用reduceByKey算子替换
 */
public class GroupByKeyOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<String,Integer>> scoreList = Arrays.asList(
				new Tuple2<String, Integer>("xuruyun" , 150),
				new Tuple2<String, Integer>("liangyongqi" , 100),
				new Tuple2<String, Integer>("wangfei" , 100),
				new Tuple2<String, Integer>("wangfei" , 80));
		
		JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(scoreList);
		rdd.groupByKey().foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {

			public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
				System.out.println(tuple._1 + " " + tuple._2);
			}
		});
		
		sc.close();
	}
}