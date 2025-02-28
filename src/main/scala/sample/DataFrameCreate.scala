package sample

import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

// case class
case class SampleSchema(id: Int, name: String, city: String)


object DataFrameCreate {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("DataFrameCreate").getOrCreate()

    val sc = spark.sparkContext;
    val hadoopConf = sc.hadoopConfiguration;
    //hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem") //NativeS3FileSystem Deprecated
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set("fs.s3a.path.style.access", "true");
    hadoopConf.set("fs.s3a.connection.establish.timeout", "6000");
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false");

    hadoopConf.set("fs.s3a.access.key", Constants.s3_access_key)
    hadoopConf.set("fs.s3a.secret.key", Constants.s3_secret_key)
    hadoopConf.set("fs.s3a.endpoint", Constants.s3_endpoint)
    // df = rdd + case class

    val rdd = sc.textFile("s3a://" + Constants.s3_bucket + "/sample-data.csv").
      map(_.split(",")).map(r => SampleSchema(r(0).toInt, r(1), r(2)))

    val df = spark.createDataFrame(rdd)
    println("Data: ")
    df.show()
  }
}
