import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * distinct算子
 * 去重复算子，会产生shuffle，尽量避免
 */
public class DinstinctOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SampleOperator").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> names = Arrays.asList("xuruyun", "liangyongqi", "wangfei", "xuruyun");
		JavaRDD<String> nameRDD = sc.parallelize(names, 1);

		nameRDD.distinct().foreach(new VoidFunction<String>() {

			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
	}
}