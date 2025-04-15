package sample

import org.apache.spark.sql.{SaveMode, SparkSession}
//import org.apache.iceberg.hadoop.HadoopCatalog

object IcebergSimple {
  def main(args: Array[String]): Unit = {
    // 创建 SparkSession
    val spark = SparkSession.builder()
      .appName("Iceberg S3 Example")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
//      .config("spark.sql.catalog.my_catalog.io-impl","org.apache.iceberg.aws.s3.S3FileIO")
//      .config("spark.sql.catalog.my_catalog.io-impl","org.apache.iceberg.hadoop.HadoopFileIO")
      .config("spark.sql.catalog.my_catalog.catalog-impl", "org.apache.iceberg.hadoop.HadoopCatalog")
      // https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/BaseMetastoreCatalog.html
      //.config("spark.sql.catalog.my_catalog.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
//      .config("spark.sql.catalog.my_catalog.type", "hadoop") // 或者使用 "hadoop" 作为类型
      .config("spark.sql.catalog.my_catalog.warehouse", "s3a://"+Constants.s3_bucket+"/iceberg") // S3 存储路径
      .getOrCreate()

    // 创建 Iceberg 库
//    spark.sql("CREATE DATABASE IF NOT EXISTS my_catalog.my_database")

    // 创建 Iceberg 表
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS my_catalog.my_database.my_table (
        |  id INT,
        |  data STRING
        |) USING iceberg
        |""".stripMargin
    )

    // 插入数据
    val data = Seq((1, "data1"), (2, "data2"))
    val df = spark.createDataFrame(data).toDF("id", "data")
    df.write
      .format("iceberg")
      .mode(SaveMode.Append)
      .save("my_catalog.my_database.my_table")

    // 查询数据
    val result = spark.sql("SELECT * FROM my_catalog.my_database.my_table")
    result.show()

    // 停止 SparkSession
    spark.stop()
  }
}
