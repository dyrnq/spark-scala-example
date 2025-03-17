package sample

import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * https://github.com/svatut59/spark-hudi-test
 */
object HudiSimple {
  def main(args: Array[String]): Unit = {

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Hudi S3 Example")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    // Create dataset
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

    df.write
      .format("hudi")
      .option("hoodie.datasource.write.partitionpath.field", "department")
      .option("hoodie.table.name", "stuff")
      .option("hoodie.timeline.server.port", 31745)

      .mode(SaveMode.Overwrite)
      .save("s3a://" + Constants.s3_bucket + "/hudi/stuff")


    val dfRead = spark.read
      .format("hudi")
      .option("hoodie.datasource.read.partitionpath.field", "department")
      .option("hoodie.table.name", "stuff")
      .load("s3a://" + Constants.s3_bucket + "/hudi/stuff")

    dfRead.show()


    // Stop Spark job
    spark.stop()
  }
}
