import org.apache.spark.{SparkContext, SparkConf}

/**
  * aggregateByKey算子演示
  * 会根据key进行聚合
  * 会根据定义的seq函数比较应该返回的值并对所有返回的值进行comb操作
  * 该算子需要注意本地执行和服务器执行会有不同，保险起见本地执行设置为*
  */
object AggregateOperator {

  def main(args: Array[String]) {

    // 设置环境参数
    val conf = new SparkConf().setAppName("AggregateOperator").setMaster("local[*]")

    // 初始化执行环境
    val sc = new SparkContext(conf)

    // 初始化并对数据进行分区操作
    val dataRdd = sc.parallelize(List((1,3),(1,2),(1,4),(2,3)))

    // 定义聚合操作
    def comb(a:Int,b:Int):Int = {
      println("comb:" + a + "\t" + b)
      a + b
    }

    // 定义比较操作
    def seq(a:Int,b:Int):Int = {
      println("seq:" + a + "\t" + b)
      math.max(a,b)
    }

    // 使用3作为中间值，并进行collect(提取所有数据)的action操作，最后进行foreach操作
    dataRdd.aggregateByKey(3)(seq,comb).collect().foreach(println)

    // 输出结果中，会存在
    // seq:3	3
    // seq:3	4
    // seq:3	3
    // seq:3 2
    // comb:3	3
    // comb:6	4
    // (1,10)
    // (2,3)
    // 会调用seq和comb对数据做处理
  }
}
