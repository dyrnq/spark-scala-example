package sample

import org.apache.spark.sql.{SaveMode, SparkSession}
//import org.apache.iceberg.hadoop.HadoopCatalog
//import org.apache.iceberg.aws.s3.S3FileIO
//import org.apache.iceberg.hadoop.HadoopFileIO

object IcebergRestSimple {
  def main(args: Array[String]): Unit = {
    // 创建 SparkSession

    val spark = SparkSession.builder()
      .appName("Iceberg S3 Example")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.rest_catalog", "org.apache.iceberg.spark.SparkCatalog")
      // https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/BaseMetastoreCatalog.html
      // https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/spark/SparkCatalog.html
      //      .config("spark.sql.catalog.rest_catalog.type", "rest")
      .config("spark.sql.catalog.rest_catalog.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
      // https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/io/FileIO.html
      .config("spark.sql.catalog.rest_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
      .config("spark.sql.catalog.rest_catalog.uri", "http://192.168.6.152:9001/iceberg/")
//      .config("spark.sql.catalog.rest_catalog.warehouse", "s3://" + Constants.s3_bucket + "/iceberg") // S3 存储路径
      .config("spark.sql.catalog.rest_catalog.s3.endpoint", "http://192.168.6.130:19000")
      .config("spark.executor.extraJavaOptions", String.format("-Daws.region=%s -Daws.accessKeyId=%s -Daws.secretAccessKey=%s",
        Constants.s3_region,
        Constants.s3_access_key,
        Constants.s3_secret_key))
      //      .config("spark.executorEnv.AWS_REGION", Constants.s3_region)
      //      .config("spark.executorEnv.AWS_ACCESS_KEY_ID", Constants.s3_access_key)
      //      .config("spark.executorEnv.AWS_SECRET_ACCESS_KEY", Constants.s3_secret_key)
      .getOrCreate()

    // 25/04/26 01:48:10 INFO CatalogUtil: Loading custom FileIO implementation: org.apache.iceberg.io.ResolvingFileIO

    // 创建 Iceberg 库
    spark.sql("SHOW CATALOGS").show()
    spark.sql("SHOW DATABASES FROM rest_catalog").show()

    // 创建 Iceberg 表
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS rest_catalog.testdb.my_table (
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
      .save("rest_catalog.testdb.my_table")

    // 查询数据
    val result = spark.sql("SELECT * FROM rest_catalog.testdb.my_table")
    result.show()

    // 停止 SparkSession
    spark.stop()
  }
}
