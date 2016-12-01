import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用sacla实现wordcount
  */
object Wc {
  def main(args: Array[String]) {
    // 设置环境参数
    val sparkConf = new SparkConf().setAppName("Wc").setMaster("local[1]")

    // 初始化执行环境
    val sc = new SparkContext(sparkConf)

    // 读取测试文件
    val r1 = sc.textFile("/private/var/root/Desktop/log.txt")

    // 使用flatMap算子对每一行使用\t分割
    val r2 = r1.flatMap(_.split("\t"))

    // 使用map算子为每一个word转换为(word,1)结构
    val r3 = r2.map {(_,1)}

    // 使用reduceByKey算子进行聚合
    val r4 = r3.reduceByKey(_+_)

    // 使用sortBy算子进行排序
    val r5 = r4.sortBy(_._2)

    // 输出结果
    r5.foreach(println(_))
  }
}
