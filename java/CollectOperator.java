import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * collect算子
 * 该算子为action算子
 * 会将数据拉到driver端，建议使用foreach，collect很容易造成driver挂掉
 */
public class CollectOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("ReduceOperator").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 有一个集合，里面有1到10，10个数字，现在我们通过reduce来进行累加
		List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> numbers = sc.parallelize(numberList);
		
		JavaRDD<Integer> doubleNumbers = numbers.map(new Function<Integer, Integer>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer call(Integer v) throws Exception {
				return v * 2;
			}
		});
		
		List<Integer> doubleNumberList = doubleNumbers.collect();
		for(Integer num : doubleNumberList){
			System.out.println(num);
		}

		sc.close();
	}
}