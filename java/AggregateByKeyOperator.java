import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

/**
 * aggregateByKey算子演示
 * 会根据key进行聚合
 * 会根据定义的第一个匿名函数对每个分区的数据进行处理
 * 会根据所有分区的返回值进行第二个匿名函数的操作
 * 该算子在分区数为1时不会调用第二个匿名函数
 */
public class AggregateByKeyOperator {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("AggregateByKeyOperator").setMaster("local[3]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		List<Tuple2<Integer,Integer>> dataList = new ArrayList<Tuple2<Integer,Integer>>();
		dataList.add(new Tuple2<Integer, Integer>(1,99));
		dataList.add(new Tuple2<Integer, Integer>(2,78));
		dataList.add(new Tuple2<Integer, Integer>(1,89));
		dataList.add(new Tuple2<Integer, Integer>(2,3));

		JavaPairRDD<Integer,Integer> dataRdd = sc.parallelizePairs(dataList);

		JavaPairRDD<Integer,Integer> aggregateByKey = dataRdd.aggregateByKey(80,new Function2<Integer,Integer,Integer>() {
			public Integer call(Integer t1,Integer t2) throws Exception {
				System.out.println("seq:" + t1 + "\t" + t2);
				return Math.max(t1,t2);
			}
		},new Function2<Integer,Integer,Integer>() {
			public Integer call(Integer t1,Integer t2) throws Exception {
				System.out.println("comb: " + t1 + "\t " + t2);
				return t1 + t2;
			}
		});
		List<Tuple2<Integer,Integer>> resultRdd = aggregateByKey.collect();
		for(Tuple2<Integer,Integer> tuple2 : resultRdd) {
			System.out.println(tuple2._1+"\t"+tuple2._2);
		}
	}
}