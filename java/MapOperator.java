import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * map算子
 * 分区中每一条数据都会调用map算子，作用一条，返回一条
 * 数据量过大时可以考虑map算子
 */
public class MapOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("MapOperator").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> names = Arrays.asList("xurunyun","liangyongqi","wangfei");
		JavaRDD<String> nameRDD = sc.parallelize(names);

		final Map<String,Integer> scoreMap = new HashMap<String,Integer>();
		scoreMap.put("xurunyun", 150);
		scoreMap.put("liangyongqi", 100);
		scoreMap.put("wangfei", 90);

		JavaRDD<Integer> scoreRDD = nameRDD.map(new Function<String,Integer>(){
			public Integer call(String v1) throws Exception {
				Integer score = scoreMap.get(v1);
				return score;
			}
		});

		scoreRDD.foreach(new VoidFunction<Integer>(){
			public void call(Integer score) throws Exception {
				System.out.println(score);
			}
		});

		sc.close();
	}
}