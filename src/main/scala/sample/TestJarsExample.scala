package sample

import cn.hutool.core.util.StrUtil
import org.apache.spark.sql.SparkSession

object TestJarsExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("TestJarsExample").getOrCreate()


    // 打印实际生效的 classpath
    val classLoader = getClass.getClassLoader
    val urls = classLoader.asInstanceOf[java.net.URLClassLoader].getURLs
    urls.foreach(url => {
      println(url)
    })

    println("##################### class print driver #########" + StrUtil.isBlankIfStr(""))


    val allConfigs = spark.sparkContext.getConf.getAll
    allConfigs.foreach(println)


    /**
     * For distributed shuffle operations like reduceByKey and join, the largest number of partitions in a parent RDD. For operations like parallelize with no parent RDDs, it depends on the cluster manager:
     * Local mode: number of cores on the local machine
     * Mesos fine grained mode: 8
     * Others: total number of cores on all executor nodes or 2, whichever is larger
     */

    val parallelism = spark.sparkContext.defaultParallelism
    println(parallelism)

    // 创建一个 Spark RDD
    val data = spark.sparkContext.makeRDD((1 to 3000000),100)
    println(s"Number of partitions:", data.getNumPartitions)


    val result = data.map(x => x * x).reduce(
      (x, y) => {
        println("##################### class print executor #########" + StrUtil.isBlankIfStr(""))
        x + y
      }

    )


    spark.stop()

  }

}
