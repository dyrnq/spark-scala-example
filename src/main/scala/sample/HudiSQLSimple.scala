package sample

import org.apache.spark.sql._
import org.apache.spark.sql.types.{DataTypes, StructType}

/**
 * https://github.com/svatut59/spark-hudi-test
 */
object HudiSQLSimple {
  def main(args: Array[String]): Unit = {

    // Create a SparkSession
    val spark = SparkSession.builder()
      .appName("Hudi S3 Example")
      .config("spark.sql.extensions","org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
      .config("spark.sql.warehouse.dir", "s3a://"+Constants.s3_bucket+"/hudi") // S3 存储路径
      .getOrCreate()

    // Create hudi database
    spark.sql("""
      CREATE DATABASE IF NOT EXISTS hudi_db  LOCATION 's3a://""" + Constants.s3_bucket + """/hudi/hudi_db';
      """)
    // Create hudi table
    spark.sql("""
      CREATE TABLE IF NOT EXISTS hudi_db.hudi_table (
        ts BIGINT,
        uuid STRING,
        rider STRING,
        driver STRING,
        fare DOUBLE,
        city STRING
      ) USING HUDI
      PARTITIONED BY (city)
      """)


    spark.sql("""
    INSERT INTO hudi_db.hudi_table
    VALUES
    (1695159649087,'334e26e9-8355-45cc-97c6-c31daf0df330','rider-A','driver-K',19.10,'san_francisco'),
    (1695091554788,'e96c4396-3fad-413a-a942-4cb36106d721','rider-C','driver-M',27.70 ,'san_francisco'),
    (1695046462179,'9909a8b1-2d15-4d3d-8ec9-efc48c536a00','rider-D','driver-L',33.90 ,'san_francisco'),
    (1695332066204,'1dced545-862b-4ceb-8b43-d2a568f6616b','rider-E','driver-O',93.50,'san_francisco'),
    (1695516137016,'e3cf430c-889d-4015-bc98-59bdce1e530c','rider-F','driver-P',34.15,'sao_paulo'    ),
    (1695376420876,'7a84095f-737f-40bc-b62f-6b69664712d2','rider-G','driver-Q',43.40 ,'sao_paulo'    ),
    (1695173887231,'3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04','rider-I','driver-S',41.06 ,'chennai'      ),
    (1695115999911,'c8abbe79-8d89-47ea-b4ce-4d224bae5bfa','rider-J','driver-T',17.85,'chennai');
    """)

    // 查询数据
    val result = spark.sql("SELECT * FROM hudi_db.hudi_table")
    result.show()


    // Stop Spark job
    spark.stop()
  }
}
