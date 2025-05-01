package sample

import org.apache.spark.sql.SparkSession

object RepartitionExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("RepartitionExample").getOrCreate()

    val allConfigs = spark.sparkContext.getConf.getAll
    allConfigs.foreach(println)

    // 创建一个 Spark RDD
    val data = spark.sparkContext.parallelize(Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

    // 输出原始 RDD 的分区信息
    println("原始 RDD 的分区信息:")
    val partitionInfo = data.mapPartitions { partition =>
      val partitionData = partition.toArray
      Iterator(s"Partition: ${partitionData.mkString(", ")}")
    }.collect()
    partitionInfo.foreach(println)


    // 获取 executor 的信息
    val executorMemoryStatus = spark.sparkContext.getExecutorMemoryStatus
    executorMemoryStatus.foreach { case (executorId, memoryStatus) =>
      println(s"Executor $executorId: ${memoryStatus}")
    }

    // 重新分区
    val repartitionedData = data.repartition(4)

    // 输出重新分区后的 RDD 的分区信息
    println("\n重新分区后的 RDD 的分区信息:")
    val repartitionedPartitionInfo = repartitionedData.mapPartitions { partition =>
      val partitionData = partition.toArray
      Iterator(s"Partition: ${partitionData.mkString(", ")}")
    }.collect()
    repartitionedPartitionInfo.foreach(println)

    // 获取 executor 的信息
    val repartitionedExecutorMemoryStatus = spark.sparkContext.getExecutorMemoryStatus
    repartitionedExecutorMemoryStatus.foreach { case (executorId, memoryStatus) =>
      println(s"Executor $executorId: ${memoryStatus}")
    }


    // 减少分区数
    val coalescedData = data.coalesce(2)

    // 输出减少分区数后的 RDD 的分区信息
    println("\n减少分区数后的 RDD 的分区信息:")
    val coalescedPartitionInfo = coalescedData.mapPartitions { partition =>
      val partitionData = partition.toArray
      Iterator(s"Partition: ${partitionData.mkString(", ")}")
    }.collect()
    coalescedPartitionInfo.foreach(println)

    // 获取 executor 的信息
    val coalescedExecutorMemoryStatus = spark.sparkContext.getExecutorMemoryStatus
    coalescedExecutorMemoryStatus.foreach { case (executorId, memoryStatus) =>
      println(s"Executor $executorId: ${memoryStatus}")
    }


    spark.stop()

  }

}
