import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * aggregate算子演示
 * 会根据定义的第一个匿名函数对每个分区的数据进行处理
 * 会根据所有分区的返回值进行第二个匿名函数的操作
 * 该算子在分区数为1时不会调用第二个匿名函数
 */
public class AggregateOperator {

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("AggregateOperator").setMaster("local[10]");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		List<Integer> dataList =  new ArrayList<Integer>();
		dataList.add(1);
		dataList.add(2);
		dataList.add(3);
		dataList.add(4);
		dataList.add(5);
		dataList.add(6);

		JavaRDD<Integer> dataRdd = sc.parallelize(dataList,3);

		JavaRDD<String> nameWithPartitonIndex = dataRdd.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {

			public Iterator<String> call(Integer index, Iterator<Integer> iterator) throws Exception {
				List<String> list = new ArrayList<String>();
				while(iterator.hasNext()){
					Integer name = iterator.next();
					String result = index + " : " + name;
					list.add(result);
				}
				return list.iterator();
			}
		}, true);
		 
		nameWithPartitonIndex.foreach(new VoidFunction<String>() {

			public void call(String result) throws Exception {
				System.out.println(result);
			}
		});
		 
		Integer result = dataRdd.aggregate(2,new Function2<Integer,Integer,Integer>() {

			public Integer call(Integer v1, Integer v2) throws Exception {
				System.out.println("seq:"+v1+"==="+v2);
				return Math.min(v1, v2);
			}
		},new Function2<Integer,Integer,Integer>() {

			public Integer call(Integer v1, Integer v2) throws Exception {
				System.out.println("comb:"+v1+"==="+v2);
				return v1+v2;
			}

		});
		
		System.out.println(result);
	}
}