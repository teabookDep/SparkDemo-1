import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * mapPartitions算子
 * 该算子会作用在整个分区上，一个分区执行一个mapPartitions，数据量过大时需要考虑替换为map算子
 */
public class MapPartitionsOperator {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("MapPartitionsOperator").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> names = Arrays.asList("xurunyun","liangyongqi","wangfei");
		JavaRDD<String> nameRDD = sc.parallelize(names);

		final Map<String, Integer> scoreMap = new HashMap<String, Integer>();
		scoreMap.put("xurunyun", 150);
		scoreMap.put("liangyongqi", 100);
		scoreMap.put("wangfei", 90);

		JavaRDD<Integer> scoreRDD = nameRDD.mapPartitions(new FlatMapFunction<Iterator<String>,Integer>(){
			public Iterable<Integer> call(Iterator<String> iterator) throws Exception {
				List<Integer> list = new ArrayList<Integer>();
				while(iterator.hasNext()) {
					String name = iterator.next();
					Integer score = scoreMap.get(name);
					list.add(score);
				}
				return list;
			}
		});

		scoreRDD.foreach(new VoidFunction<Integer>() {
			public void call(Integer score) throws Exception {
				System.out.println(score);
			}
		});

		sc.close();
	}
}