import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
  * 用于说明持久化在内存的效果，持久化操作是懒执行操作
  */
object Cache {

  def main(args: Array[String]) {

    // 设置环境参数
    val sparkConf = new SparkConf().setAppName("CachedTest").setMaster("local")

    // 初始化执行环境
    val sc = new SparkContext(sparkConf)

    // 初始化数据
    var linesRdd = sc.textFile("hs_err_pid5848.log")

    // 持久化操作，或者使用linesRdd.cache()代替下面的操作
    linesRdd = linesRdd.persist(StorageLevel.MEMORY_ONLY)

    // 拿到当前时间
    val startTime = System.currentTimeMillis()

    // 做count操作
    val lineCount = linesRdd.count()

    // 拿到运行后时间
    val endTime = System.currentTimeMillis()

    // 输出运行时长
    println("总共有：" + lineCount + "条记录，计算耗时：" + (endTime - startTime))

    // 拿到当前时间
    val startCachedTime = System.currentTimeMillis()

    // 再次做count操作
    val linesCountCached = linesRdd.count()

    // 拿到运行后时间
    val endCachedTime = System.currentTimeMillis()

    // 和上面的时间进行比较
    println("总共有：" + linesCountCached + "条记录，计算耗时：" + (endCachedTime - startCachedTime))
  }
}
