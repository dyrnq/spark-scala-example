package sample

import cn.hutool.core.io.FileUtil
import cn.hutool.core.util.StrUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkFiles, TaskContext}

import java.io.File
import java.nio.charset.Charset

object TestJarsExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("TestJarsExample").getOrCreate()

    println("**************************** driver getRootDirectory=" + SparkFiles.getRootDirectory())

    val file = new File(".")
    println("**************************** driver . =" + file.getAbsolutePath)
    file.listFiles().foreach { f =>
      println(f.getName)
    }

    if (args.length > 0) {
      println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^" + args(0));
      println(FileUtil.readString(new File(SparkFiles.getRootDirectory() + "/spark-defaults.conf"), Charset.defaultCharset()))
    }

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
    val data = spark.sparkContext.makeRDD((1 to 3000000), 100)
    println(s"Number of partitions:", data.getNumPartitions)


    val result = data.map(x => {
      println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ map " + StrUtil.isBlankIfStr(""))
      x * x
    }).reduce(

      (x, y) => {

        if (TaskContext.get != null) {
          val partitionId = TaskContext.get.partitionId
          println("$$$$$$$$$$$$$$$$$$$$$ partitionId=" + partitionId);
        }
        println("##################### class print reduce #########" + StrUtil.isBlankIfStr(""))
        println("**************************** reduce getRootDirectory=" + SparkFiles.getRootDirectory())
        val file = new File(SparkFiles.getRootDirectory())
        println("**************************** reduce" + file.getAbsolutePath)
        file.listFiles().foreach { f =>
          println(f.getName)
        }

        if (args.length > 0) {
          println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^" + args(0));
          println(FileUtil.readString(new File(SparkFiles.getRootDirectory() + "/spark-defaults.conf"), Charset.defaultCharset()))
        }

        x + y
      }

    )


    spark.stop()

  }

}
