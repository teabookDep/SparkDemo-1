import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * countByKey算子
 * 在count的基础上在driver端对key进行聚合，不建议使用
 */
public class CountByKeyOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("CountByKeyOperator")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<String,String>> scoreList = Arrays.asList(
				new Tuple2<String,String>("70s","xuruyun"),
				new Tuple2<String,String>("70s","wangfei"),
				new Tuple2<String,String>("70s","wangfei"),
				new Tuple2<String,String>("80s","yasaka"),
				new Tuple2<String,String>("80s","zhengzongwu"),
				new Tuple2<String,String>("80s","lixin"));
		JavaPairRDD<String,String> students = sc.parallelizePairs(scoreList);

		Map<String, Object> counts = students.countByKey();
		for(Map.Entry<String, Object> studentCount : counts.entrySet()){
			System.out.println(studentCount.getKey() + ": " +  studentCount.getValue());
		}
		
		sc.close();
	}
}