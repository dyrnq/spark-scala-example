package sample

import org.apache.spark.sql.SparkSession

object WordCount {
  val myAccessKey = "xxxXXX"
  val mySecretKey = "xxxXXXxxxXXX"
  val endpoint = "192.168.6.130:19000"
  val bucket = "bigdata"
  val filepath = "your_path.txt"

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("WordCount")
      .getOrCreate()

    val sc = spark.sparkContext;
    val hadoopConf = sc.hadoopConfiguration;
    //hadoopConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem") //NativeS3FileSystem Deprecated
    hadoopConf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoopConf.set("fs.s3a.path.style.access", "true");
    hadoopConf.set("fs.s3a.connection.establish.timeout", "6000");
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false");

    hadoopConf.set("fs.s3a.access.key", myAccessKey)
    hadoopConf.set("fs.s3a.secret.key", mySecretKey)
    hadoopConf.set("fs.s3a.endpoint", endpoint)


    val s3data = sc.textFile("s3a://" + bucket + "/" + filepath)
    val numDog = s3data.filter(line => line.contains("Dog")).count()
    val numCat = s3data.filter(line => line.contains("Cat")).count()
    val total = s3data.count()
    println("total lines: %s".format(total))
    println("Lines with Dog: %s, Lines with Cat: %s".format(numDog, numCat))
    sc.stop()
  }
}

// https://gist.github.com/kakakazuma/d6977b8fbd7c48c39c65
// https://medium.com/@ramachandrankrish/integrating-org-apache-hadoop-fs-s3a-s3afilesystem-to-access-the-aws-s3-bucket-via-spark-java-3744ffadb60d