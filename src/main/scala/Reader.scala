package sn.airbnb.ml

import org.apache.spark.sql.{DataFrame, SparkSession}

object Reader {
  def read(spark: SparkSession, path: String): DataFrame = {
    val rawDF = spark.read
      .option("header", "true")
      .option("multiLine", "true")
      .option("inferSchema", "true")
      .option("escape", "\"")
      .csv(path)
    rawDF
  }
}
