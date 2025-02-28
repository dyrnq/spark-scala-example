package sample

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object FixCreateDF {
  def main(args: Array[String]): Unit = {
    // 创建 SparkSession
    val spark = SparkSession.builder().appName("FixCreateDF").getOrCreate()

    // 示例数据（确保 RDD 元素为 Row 类型）
    val rdd = spark.sparkContext.parallelize(Seq(
      Row("Alice", 25), // 必须使用 Row 包装数据
      Row("Bob", 30),
      Row("Charlie", 35)
    ))

    // 显式定义 Schema（必须为 StructType）
    val schema = StructType(Seq(
      StructField("name", StringType, nullable = false),
      StructField("age", IntegerType, nullable = false)
    ))

    // 正确调用 createDataFrame
    val df = spark.createDataFrame(rdd, schema) // 参数顺序：RDD[Row] + Schema

    df.show()
  }
}
