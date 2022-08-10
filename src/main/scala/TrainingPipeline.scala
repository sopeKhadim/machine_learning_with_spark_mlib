package sn.airbnb.ml

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame

object TrainingPipeline {
  def train(df: DataFrame): PipelineModel = {

    val categoricalCols = df.dtypes.filter(_._2 == "StringType").map(_._1)
    val indexOutputCols = categoricalCols.map(_ + "Index")
    val oheOutputCols = categoricalCols.map(_ + "OHE")

    val stringIndexerArray = categoricalCols.map(columnName => {
      new StringIndexer()
        .setInputCol(columnName)
        .setOutputCol(s"${columnName}Index")
        .setHandleInvalid("skip")
    })


    val oheEncoder = new OneHotEncoder()
      .setInputCols(indexOutputCols)
      .setOutputCols(oheOutputCols)

    val numericCols = df.dtypes.filter{ case (field, dataType) =>
      dataType == "DoubleType" && field != "price" && field != "log_price"}.map(_._1)
    val assemblerInputs = oheOutputCols ++ numericCols
    val vecAssembler = new VectorAssembler()
      .setInputCols(assemblerInputs)
      .setOutputCol("features")

    val lr = new LinearRegression()
      .setLabelCol("log_price")
      .setFeaturesCol("features")
      .setPredictionCol("log_pred")

    val stages = stringIndexerArray ++ Array(oheEncoder, vecAssembler, lr)

    val pipeline = new Pipeline()
      .setStages(stages)
    val pipelineModel = pipeline.fit(df)
    pipelineModel
    //    val predDF = pipelineModel.transform(logTestDF)
  }
}
