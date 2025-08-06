package sample

import org.apache.spark.sql.SparkSession

object GravitinoSparkSimple {
  def main(args: Array[String]): Unit = {
    // 创建 SparkSession

    val spark = SparkSession.builder()
      .appName("GravitinoSparkSimple")
      .config("spark.plugins", "org.apache.gravitino.spark.connector.plugin.GravitinoSparkPlugin")
      .config("spark.sql.gravitino.uri","http://192.168.6.159:8090")
      .config("spark.sql.gravitino.metalake","mylake")
      .getOrCreate()


    // 创建 Iceberg 库
    spark.sql("SHOW CATALOGS").show()
    spark.sql("SHOW DATABASES FROM foo_rest_catalog").show()
    spark.sql("SHOW DATABASES FROM bar_rest_catalog").show()
    //    spark.sql("select * from bar_rest_catalog.bar.bar_table;")
    spark.sql(
      """
        |SELECT * FROM
        |bar_rest_catalog.bar.bar_table as bar,
        |foo_rest_catalog.foo.foo_table as foo
        |WHERE bar.foo_id=foo.id;
        |""".stripMargin).show()

    // 停止 SparkSession
    spark.stop()
  }

}
