package sn.airbnb.ml

object Main extends SparkSessionTrait {
  def main(args: Array[String]): Unit = {
    val filePath = "/home/dmboup/gitProjects/advanced_functional_programming/datasets/sf-airbnb/sf-airbnb.csv"
    val cleanDF = Reader.read(spark, filePath)
      .transform(Preprocessor.preprocess)

    cleanDF.cache().count()

    //    cleanDF.show(false)

    val Array(trainDF, testDF) = Utils.trainTestSplit(cleanDF)

    val logTrainDF = trainDF.transform(Utils.logPrice)
    val logTestDF = testDF.transform(Utils.logPrice)

    val pipelineModel = TrainingPipeline.train(logTrainDF)
    val logTrainPredDF = pipelineModel.transform(logTrainDF)
    val logTestPredDF = pipelineModel.transform(logTestDF)
    //    logTestPredDF.select("features", "price", "log_price", "log_pred")
    //      .show(10)

    val expTrainPredDF = logTrainPredDF.transform(Utils.expPrice)
    val expTestPredDF = logTestPredDF.transform(Utils.expPrice)

    val Array((rmseTrain, rmseTest), (r2Train, r2Test))  = Evaluator.evaluate(expTrainPredDF, expTestPredDF)
    println("*-"*60)
    println(s"(rmseTrain, rmseTest) = ${(rmseTrain, rmseTest)}, (r2Train, r2Test) = ${(r2Train, r2Test)}")
    println("*-"*60)

    val pipelinePath = "modeles/lr-pipeline-model"
    Utils.saveModel(pipelineModel, pipelinePath)
  }
}
