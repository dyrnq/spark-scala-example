package sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OrdersAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OrdersAnalysis")
      .getOrCreate()

    // 创建 orders 表
    //    val orders = spark.createDataFrame(Seq(
    //      (1, 1, "2022-01-01 12:00:00", "pending", 100.0),
    //      (2, 2, "2022-01-01 12:01:00", "paid", 200.0),
    //      (3, 3, "2022-01-01 12:02:00", "pending", 300.0),
    //      (4, 4, "2022-01-01 12:03:00", "paid", 400.0),
    //      (5, 5, "2022-01-01 12:04:00", "pending", 500.0),
    //      (6, 6, "2022-01-01 12:05:00", "paid", 600.0),
    //      (7, 7, "2022-01-01 12:06:00", "pending", 700.0),
    //      (8, 8, "2022-01-01 12:07:00", "paid", 800.0),
    //      (9, 9, "2022-01-01 12:08:00", "pending", 900.0),
    //      (10, 10, "2022-01-01 12:09:00", "paid", 1000.0)
    //    )).toDF("id", "customer_id", "order_date", "status", "total_amount")


    val orders = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("dateFormat", "yyyy-MM-dd HH:mm:ss").load("s3a://" + Constants.s3_bucket + "/OrdersAnalysis.csv")


    // 将 order_date 转换为 Timestamp 类型
    val ordersWithTimestamp = orders.withColumn("order_date", to_timestamp(col("order_date")))

    // 使用 window 函数来计算待支付订单对比总订单比例
    val windowedOrders = ordersWithTimestamp
      .withColumn("window", window(col("order_date"), "3 minutes", "1 minute"))
      .groupBy("window")
      .agg(
        count(when(col("status") === "pending", col("id"))).alias("pending_count"),
        count(col("id")).alias("total_count")
      )
      .withColumn("ratio", col("pending_count") / col("total_count"))

    // 过滤出比例超过 40% 的窗口
    val alerts = windowedOrders.filter(col("ratio") > 0.4)

    // 打印出 alerts
    alerts.show(false)

  }
}
