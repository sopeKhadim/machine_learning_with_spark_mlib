package sn.airbnb.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.DataFrame

object Evaluator {
  def evaluate(trainPredDF: DataFrame, testPredDF: DataFrame): Array[(Double, Double)] = {
    val regressionEvaluator = new RegressionEvaluator()
      .setPredictionCol("prediction")
      .setLabelCol("price")

    val rmseTrain = regressionEvaluator.setMetricName("rmse").evaluate(trainPredDF)
    val rmseTest = regressionEvaluator.setMetricName("rmse").evaluate(testPredDF)

    val r2Train = regressionEvaluator.setMetricName("r2").evaluate(trainPredDF)
    val r2Test = regressionEvaluator.setMetricName("r2").evaluate(testPredDF)
    Array((rmseTrain, rmseTest), (r2Train, r2Test))
  }
}
