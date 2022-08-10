package sn.airbnb.ml

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionTrait {

  val sparkConf: SparkConf =
    new SparkConf()
      .setAppName("Predict San Francisko Airbnb Price")
      .setMaster("local[4]")

  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .config(sparkConf)
      //      .enableHiveSupport()
      .getOrCreate()
  }
}
