package sample

import org.apache.spark.sql.{SaveMode, SparkSession}

object IcebergRestMultiSupportSimple {
  def main(args: Array[String]): Unit = {
    // 创建 SparkSession

    val spark = SparkSession.builder()
      .appName("Iceberg S3 Example")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.foo_rest_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.foo_rest_catalog.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
      .config("spark.sql.catalog.foo_rest_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
      .config("spark.sql.catalog.foo_rest_catalog.uri", "http://192.168.6.159:29001/iceberg")
      .config("spark.sql.catalog.foo_rest_catalog.prefix", "foo")
      .config("spark.sql.catalog.foo_rest_catalog.s3.endpoint", "http://192.168.6.159:9000")
      .config("spark.sql.catalog.foo_rest_catalog.s3.path-style-access", "true")
      .config("spark.sql.catalog.foo_rest_catalog.client.credentials-provider", "sample.CustomCredentialProvider")
      .config("spark.sql.catalog.foo_rest_catalog.client.region", "us-east-1")
      .config("spark.sql.catalog.foo_rest_catalog.client.credentials-provider.accessKeyId", "vUR3oLMF5ds8gWCP")
      .config("spark.sql.catalog.foo_rest_catalog.client.credentials-provider.secretAccessKey", "odWFIZukYrw9dY0G5ezDKMZWbhU0S4oD")


      .config("spark.sql.catalog.bar_rest_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.bar_rest_catalog.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
      .config("spark.sql.catalog.bar_rest_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
      .config("spark.sql.catalog.bar_rest_catalog.uri", "http://192.168.6.159:29001/iceberg")
      .config("spark.sql.catalog.bar_rest_catalog.prefix", "bar")
      .config("spark.sql.catalog.bar_rest_catalog.s3.endpoint", "http://192.168.6.159:9200")
      .config("spark.sql.catalog.bar_rest_catalog.s3.path-style-access", "true")
      .config("spark.sql.catalog.bar_rest_catalog.client.credentials-provider", "sample.CustomCredentialProvider")
      .config("spark.sql.catalog.bar_rest_catalog.client.region", "us-east-1")
      .config("spark.sql.catalog.bar_rest_catalog.client.credentials-provider.accessKeyId", "vUR3oLMF5ds8gWCP")
      .config("spark.sql.catalog.bar_rest_catalog.client.credentials-provider.secretAccessKey", "odWFIZukYrw9dY0G5ezDKMZWbhU0S4oD")
      .getOrCreate()


    // 创建 Iceberg 库
    spark.sql("SHOW CATALOGS").show()
    spark.sql("SHOW DATABASES FROM foo_rest_catalog").show()
    spark.sql("SHOW DATABASES FROM bar_rest_catalog").show()


    // 停止 SparkSession
    spark.stop()
  }

}
