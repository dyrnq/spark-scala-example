package sample

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

// <https://github.com/parmarsachin/spark-dataframe-demo/blob/master/src/main/scala/com/sachinparmar/meetup/spark/dataframe/dfExplain.scala>
object DataFrameForJoin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("DataFrameForJoin").getOrCreate()

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

    val empDFSchema =
      StructType(
        StructField("emp_id", IntegerType, nullable = false) ::
          StructField("emp_name", StringType, nullable = true) ::
          StructField("salary", IntegerType, nullable = true) ::
          StructField("age", IntegerType, nullable = true) :: Nil)
    val empDF = spark.read.schema(empDFSchema).json("s3a://" + Constants.s3_bucket + "/emp.json")

    val deptDFSchema =
      StructType(
        StructField("dept_id", IntegerType, nullable = false) ::
          StructField("dept_name", StringType, nullable = true) :: Nil)
    val deptDF = spark.read.schema(deptDFSchema).json("s3a://" + Constants.s3_bucket + "/dept.json")

    val registerDFSchema =
      StructType(
        StructField("emp_id", IntegerType, nullable = false) ::
          StructField("dept_id", IntegerType, nullable = true) :: Nil)
    val registerDF = spark.read.schema(registerDFSchema).json("s3a://" + Constants.s3_bucket + "/register.json")

    // df
    val df = empDF.
      join(registerDF, registerDF("emp_id") === empDF("emp_id")).
      select(empDF("emp_id"), registerDF("dept_id"), upper(lower(empDF("emp_name"))).as("emp_name"), empDF("salary"), empDF("age")).
      join(deptDF, registerDF("dept_id") === deptDF("dept_id")).
      select("emp_id", "salary", "dept_name", "emp_name").
      filter("salary >= 2000").
      filter("salary < 5000")


    // cdf
    val cdf = empDF.
      cache().
      join(registerDF, registerDF("emp_id") === empDF("emp_id")).
      select(empDF("emp_id"), registerDF("dept_id"), upper(lower(empDF("emp_name"))).as("emp_name"), empDF("salary"), empDF("age")).
      join(deptDF, registerDF("dept_id") === deptDF("dept_id")).
      select("emp_id", "salary", "dept_name", "emp_name").
      filter("salary >= 2000").
      filter("salary < 5000")


    println("\n\n logical and physical plans with cache \n\n")

    println("\n DF analyzed : \n\n" + df.queryExecution.analyzed.numberedTreeString)
    println("\n DF(Cache) analyzed : \n\n" + cdf.queryExecution.analyzed.numberedTreeString)

    println("\n DF optimizedPlan : \n\n" + df.queryExecution.optimizedPlan.numberedTreeString)
    println("\n DF(Cache) optimizedPlan : \n\n" + cdf.queryExecution.optimizedPlan.numberedTreeString)

    println("\n DF sparkPlan : \n\n" + df.queryExecution.sparkPlan.numberedTreeString)
    println("\n DF(Cache) sparkPlan : \n\n" + cdf.queryExecution.sparkPlan.numberedTreeString)


    df.show();
    cdf.show();

  }
}
