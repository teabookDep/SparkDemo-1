import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * filter算子
 * 该算子为过滤算子，不符合标准的数据可以舍弃
 */
public class FilterOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);

		JavaRDD<Integer> results = numberRDD.filter(new Function<Integer, Boolean>() {

			public Boolean call(Integer number) throws Exception {
				return number % 2 == 0;
			}
		});
		
		results.foreach(new VoidFunction<Integer>() {

			public void call(Integer result) throws Exception {
				System.out.println(result);
			}
		});

		sc.close();
	}
}