package sample

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OrdersAnalysisKafka {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("OrdersAnalysisKafka").getOrCreate()

    // 定义时间窗口
    val windowDuration = "3 minutes"
    val windowSlideDuration = "1 minute"
    val watermark = "10 seconds"
    val checkpointLocation = "s3a://bigdata/spark-checkpoints/orders";

    val schema = StructType(Seq(
      StructField("id", LongType, nullable = false),
      StructField("customer_id", IntegerType, nullable = false),
      StructField("order_date", StringType, nullable = false),
      StructField("status", StringType, nullable = false),
      StructField("total_amount", DoubleType, nullable = false)
    ))
    //id,customer_id,order_date,status,total_amount

    // 添加其他字段

    // 加载订单数据
    val orders = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", Constants.kafka_bootstrap_servers)
      //      .option("kafka.group.id", "your_group_id")
      .option("subscribe", "orders")
      //      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING) as json_value")
      .select(from_json(col("json_value"), schema).alias("data"))
      .select("data.*")
      .withColumn("timestamp", to_timestamp(col("order_date")))
      .withWatermark("timestamp", watermark)

    //    val query = orders.writeStream
    //      .foreachBatch { (df: DataFrame, epochId: Long) =>
    //        df.show(true)
    //      }
    //      .option("checkpointLocation", checkpointLocation)
    //      .outputMode("append")
    //      .start()

    //        // 计算待支付订单数量
    //        val pendingOrders = orders.filter(col("status") === "pending")
    //          .groupBy(window(col("timestamp"), windowDuration, windowSlideDuration))
    //          .count()
    //          .withColumnRenamed("count", "pending_count")
    //
    //        // 计算总订单数量
    //        val totalOrders = orders.groupBy(window(col("timestamp"), windowDuration, windowSlideDuration))
    //          .count()
    //          .withColumnRenamed("count", "total_count")
    //
    //        // 计算比例
    //        val ratio = pendingOrders.join(totalOrders, Seq("window"))
    //          .withColumn("ratio", col("pending_count") / col("total_count"))
    //
    //
    val windowedOrders = orders
      .withColumn("window", window(col("timestamp"), windowDuration, windowSlideDuration))
      .groupBy("window")
      .agg(
        count(when(col("status") === "pending", col("id"))).alias("pending_count"),
        count(col("id")).alias("total_count")
      )
      .withColumn("ratio", col("pending_count") / col("total_count"))
      .withColumn("window_start", col("window").getField("start"))
      .withColumn("window_end", col("window").getField("end"))


    val query = windowedOrders.writeStream
      .foreachBatch { (df: DataFrame, epochId: Long) =>
        df.show(false)
      }
      .option("checkpointLocation", checkpointLocation)
      .outputMode("update")
      .start()


    //    // 判断比例是否超过80%
    //    val alerts = windowedOrders.filter(col("ratio") > 0.8)
    //
    //    // 触发报警
    //    val query = alerts.writeStream
    //      .foreachBatch { (df: DataFrame, epochId: Long) =>
    //        df.foreach(row => {
    //          val windowStart = row.getAs[java.sql.Timestamp]("window_start")
    //          val windowEnd = row.getAs[java.sql.Timestamp]("window_end")
    //          val ratio = row.getAs[Double]("ratio")
    //          // 触发报警
    //          sendAlert(windowStart, windowEnd, ratio)
    //        })
    //      }
    //      .option("checkpointLocation", checkpointLocation)
    //      .outputMode("complete") //需要将输出模式改为 complete 或 update，因为 append 输出模式不支持流式聚合操作。
    //      .start()

    query.awaitTermination()
  }

  private def sendAlert(windowStart: java.sql.Timestamp, windowEnd: java.sql.Timestamp, ratio: Double): Unit = {
    // 触发报警
    println(s"报警：$windowStart - $windowEnd，待支付订单对比总订单比例：$ratio")
  }
}


