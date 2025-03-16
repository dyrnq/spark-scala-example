package sample


import io.delta.tables.DeltaTable
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * https://github.com/vdonthireddy/spark-java-minio-delta/blob/main/src/main/java/com/niharsystems/Main.java
 */
object DeltaSimple {
  def main(args: Array[String]): Unit = {
    val deltaTableBucket = Constants.s3_bucket
    val deltaWarehouse = "s3a://" + deltaTableBucket + "/delta"
    val deltaTablePath = deltaWarehouse + "/stuff"
    val sampleJson = "s3a://" + deltaTableBucket + "/sample.json"

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Delta S3 Example")
      //      .master("local[*]")
      //      .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.3.0")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      //      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      //      .config("spark.hadoop.fs.s3a.access.key", "2nZqqHPWEzu9JooKNoXO")
      //      .config("spark.hadoop.fs.s3a.secret.key", "DfFaWePTJsp5mB50pS2a7Iz00A6AgJEmdXWGyIOx")
      //      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      //      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      //      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      //      .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
      .getOrCreate()

    //Read data from some json file
    val df1 = spark.read
      .option("multiline", "false")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .json(sampleJson)
    df1.printSchema()
    df1.select("user.address.zip").show()


    //Create dataset

    val data = spark.sparkContext.parallelize(Seq(
      Row(1, "Vijay Donthireddy", 50, "Engineering", 80, false),
      Row(2, "Kavitha Padera", 47, "Manager", 80, false),
      Row(3, "Nihar Donthireddy", 18, "College", 80, false),
      Row(4, "Nirav1 Donthireddy", 12, "Middle School", 83, false),
      Row(5, "Nirav2 Donthireddy", 12, "High School", 90, false),
      Row(6, "Nirav3 Donthireddy", 12, "Elementary School", 83, false),
      Row(7, "Nirav4 Donthireddy", 12, "University of CA LA", 76, false)
    ))

    // Create schema for the above dataset
    val schema1 = new StructType()
      .add("id", DataTypes.IntegerType, false)
      .add("name", DataTypes.StringType, false)
      .add("age", DataTypes.IntegerType, false)
      .add("department", DataTypes.StringType, false)
      .add("marks", DataTypes.IntegerType, false)
      .add("isdeleted", DataTypes.BooleanType, false)

    val df = spark.createDataFrame(data, schema1)
    df.printSchema()

    df.write.format("delta").mode("append").save(deltaTablePath)

    //Read data from minio//Read data from minio
    //    val dfRead = spark.read.format("delta").load(deltaTablePath)
    //    dfRead.show()

    val dt = DeltaTable.forPath(spark, deltaTablePath)
    val df2 = dt.toDF
    df2.show()
    val filteredDf = df2.filter(df2("age") > 13)
    filteredDf.show()
    //stop spark job
    spark.stop()
  }
}