package sample

import cn.hutool.core.io.FileUtil
import cn.hutool.core.util.StrUtil
import com.google.gson.GsonBuilder
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkFiles, TaskContext}

import java.io.File
import java.net.InetAddress
import java.nio.charset.Charset

object TestJarsExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("TestJarsExample").getOrCreate()
    println(args.length)

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
    println("##################### getClass.getClassLoader BEGIN #########")
    // 打印实际生效的 classpath
    val classLoader = getClass.getClassLoader
    val urls = classLoader.asInstanceOf[java.net.URLClassLoader].getURLs
    urls.foreach(url => {
      println(url)
    })
    println("##################### getClass.getClassLoader END #########")

    println("##################### class print driver #########" + StrUtil.isBlankIfStr(""))

    println("##################### getConf.getAll BEGIN #########")
    val allConfigs = spark.sparkContext.getConf.getAll
    allConfigs.foreach(println)
    println("##################### getConf.getAll END #########")

    println("##################### listJars BEGIN #########")
    spark.sparkContext.listJars().foreach(println)
    println("##################### listJars END #########")


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

    val arr3 = Array.fill(5)(0) // [0,0,0,0,0]
    val arr4 = Array.tabulate(5)(i => i * 2) // [0,2,4,6,8]
    val gson = new GsonBuilder().setPrettyPrinting().create()
    println(gson.toJson(arr4))

    val result = data.map(x => {
      println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ map " + StrUtil.isBlankIfStr(""))
      val gson = new GsonBuilder().setPrettyPrinting().create()
      println(gson.toJson(x * x))
      x * x
    }).reduce(

      (x, y) => {

        if (TaskContext.get != null) {
          val stageInfo = TaskContext.get().stageId()
          val partitionId = TaskContext.get.partitionId
          println(s"Reduce task in Stage $stageInfo, Partition $partitionId")
        }
        println("##################### class print reduce #########" + StrUtil.isBlankIfStr(""))
        println("**************************** reduce getRootDirectory=" + SparkFiles.getRootDirectory())
        val file = new File(SparkFiles.getRootDirectory())
        println("**************************** reduce " + file.getAbsolutePath)
        file.listFiles().foreach { f =>
          if (f.isDirectory) {
            f.listFiles().foreach { ff => {
              println(ff.getName)
            }
            }
          }else{
            println(f.getName)
          }

        }

        if (args.length > 0) {
          println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^" + args(0));
          println(FileUtil.readString(new File(SparkFiles.get("spark-defaults.conf")), Charset.defaultCharset()))
        }

        x + y
      }

    )
    //错误写法
    //println(FileUtil.readString(new File("spark-defaults.conf"), Charset.defaultCharset()))
    //println(FileUtil.readString(new File("./spark-defaults.conf"), Charset.defaultCharset()))

//    Exception in thread "main" java.lang.reflect.InvocationTargetException
//    at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
//    at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(Unknown Source)
//    at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(Unknown Source)
//    at java.base/java.lang.reflect.Method.invoke(Unknown Source)
//    at org.apache.spark.deploy.worker.DriverWrapper$.main(DriverWrapper.scala:63)
//    at org.apache.spark.deploy.worker.DriverWrapper.main(DriverWrapper.scala)
//    Caused by: cn.hutool.core.io.IORuntimeException: File not exist: spark-defaults.conf
//    at cn.hutool.core.io.file.FileReader.checkFile(FileReader.java:300)
//    at cn.hutool.core.io.file.FileReader.<init>(FileReader.java:57)
//    at cn.hutool.core.io.file.FileReader.create(FileReader.java:37)
//    at cn.hutool.core.io.FileUtil.readString(FileUtil.java:2173)
//    at sample.TestJarsExample$.main(TestJarsExample.scala:107)
//    at sample.TestJars

    //正确写法
    println(FileUtil.readString(new File(SparkFiles.get("spark-defaults.conf")), Charset.defaultCharset()))

    spark.stop()

  }

}
