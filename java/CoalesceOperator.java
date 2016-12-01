import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * coalesce算子
 * 重新分区，根据呃第二个参数的值决定是否产生shuffle
 * 一般应用于filter算子之后，避免数据倾斜
 */
public class CoalesceOperator {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("CoalesceOperator").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<String> staffList = Arrays.asList("xuruyun1","xuruyun2","xuruyun3"
				,"xuruyun4","xuruyun5","xuruyun6"
				,"xuruyun7","xuruyun8","xuruyun9"
				,"xuruyun10","xuruyun11","xuruyun12"
				,"xuruyun13","xuruyun14","xuruyun15"
				,"xuruyun16","xuruyun17","xuruyun18"
				,"xuruyun19","xuruyun20","xuruyun21"
				,"xuruyun22","xuruyun23","xuruyun24");
		JavaRDD<String> staffRDD = sc.parallelize(staffList, 6);
		
		JavaRDD<String> staffRDD2 = staffRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
				List<String> list = new ArrayList<String>();
				while(iterator.hasNext()){
					String staff = iterator.next();
					list.add("部门["+(index)+"]"+staff);
				}
				return list.iterator();
			}
		}, false);
		for(String staffInfo : staffRDD2.collect()){
			System.out.println(staffInfo);
		}

		JavaRDD<String> staffRDD3 = staffRDD2.coalesce(12,true);

		String debugString = staffRDD3.toDebugString();
		System.out.println(debugString);
		
		JavaRDD<String> staffRDD4 = staffRDD3.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {

			public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
				List<String> list = new ArrayList<String>();
				while(iterator.hasNext()){
					String staff = iterator.next();
					list.add("部门["+(index)+"]"+staff);
				}
				return list.iterator();
			}
		}, true);
		for(String staffInfo : staffRDD4.collect()){
			System.out.println(staffInfo);
		} 
	}
}