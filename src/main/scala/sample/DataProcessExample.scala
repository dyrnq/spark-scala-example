package sample

import org.apache.spark.sql.SparkSession;

object DataProcessExample {



  def main(args: Array[String]) {
    // 创建一个 SparkSession
    val spark = SparkSession.builder.appName("DataProcessExample").getOrCreate()

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

    val dataFrame1 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("dateFormat", "yyyy-MM-dd HH:mm:ss").load( "s3a://" + Constants.s3_bucket + "/100_Sales_Records.csv")

    val dataFrame2 = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("dateFormat", "yyyy-MM-dd HH:mm:ss").load("s3a://" + Constants.s3_bucket + "/1000_Sales_Records.csv")
    //dataFrame1.printSchema();

    dataFrame1.createTempView("salesRecord1")
    //dataFrame1 = sparkSession.sql("select * from salesRecord1");
    //dataFrame1.show();

    dataFrame2.createTempView("salesRecord2")
    //dataFrame2 = sparkSession.sql("select * from salesRecord2");
    //dataFrame2.show();

    //Dataset<Row> dataFrame3 = null;
    //dataFrame3.createOrReplaceTempView("salesRecordJoin");
    spark.sql("select * from salesRecord2").show(10)
    //sparkSession.sql("select t1.Region, t1.Country, t1.Order ID from salesRecord2 t1 join on salesRecord1 t2 where t2.Region=t1.Region").show(10);
    //dataFrame3.show();
  }
}
