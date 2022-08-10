package sn.airbnb.ml

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, exp, log}

object Utils {
  def trainTestSplit(df: DataFrame): Array[DataFrame] = {
    val Array(trainDF, testDF) = df.randomSplit(Array(.8, .2), seed=28)
    Array(trainDF, testDF)
  }

  def logPrice(df: DataFrame): DataFrame = {
    df.withColumn("log_price", log(col("price")))
  }

  def expPrice(df: DataFrame): DataFrame = {
    val expDF = df.withColumn("prediction", exp(col("log_pred")))
    expDF
  }

  def saveModel(pipelineModel: PipelineModel, path: String): Unit = {
    pipelineModel
      .write
      .overwrite()
      .save(path)
  }
}
