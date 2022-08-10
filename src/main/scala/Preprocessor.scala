package sn.airbnb.ml

import org.apache.spark.ml.feature.Imputer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, translate, when}
import org.apache.spark.sql.types.IntegerType


object Preprocessor {
  def preprocess(df: DataFrame): DataFrame = {
    df.transform(selectFeatures)
      .transform(fixPrice)
      .transform(convertIntColsToDouble)
      .transform(inputMissingCols)
      .transform(removeOutliers)
  }

  private def selectFeatures(df: DataFrame): DataFrame = {
    df.select(
      "host_is_superhost",
      "cancellation_policy",
      "instant_bookable",
      "host_total_listings_count",
      "neighbourhood_cleansed",
      "latitude",
      "longitude",
      "property_type",
      "room_type",
      "accommodates",
      "bathrooms",
      "bedrooms",
      "beds",
      "bed_type",
      "minimum_nights",
      "number_of_reviews",
      "review_scores_rating",
      "review_scores_accuracy",
      "review_scores_cleanliness",
      "review_scores_checkin",
      "review_scores_communication",
      "review_scores_location",
      "review_scores_value",
      "price")
  }

  private def fixPrice(df: DataFrame): DataFrame = {
    df.withColumn("price", translate(col("price"), "$,", "").cast("double"))
  }

  private def convertIntColsToDouble(df: DataFrame): DataFrame = {
    val integerColumns = for (x <- df.schema.fields if (x.dataType == IntegerType)) yield x.name
    var doublesDF = df

    for (c <- integerColumns)
      doublesDF = doublesDF.withColumn(c, col(c).cast("double"))
    doublesDF
  }

  private def inputMissingCols(df: DataFrame): DataFrame = {
    val missingCols = df.columns.map(c => (c, df.filter(col(c).isNull || col(c).isNaN).count()))
      .filter(_._2 > 0)
      .map(_._1)
    var doublesDF = df

    for (c <- missingCols)
      doublesDF = doublesDF.withColumn(c + "_na", when(col(c).isNull, 1.0).otherwise(0.0))

    val imputer = new Imputer()
      .setStrategy("median")
      .setInputCols(missingCols)
      .setOutputCols(missingCols)

    val imputedDF = imputer.fit(doublesDF).transform(doublesDF)
    imputedDF
  }

  private def removeOutliers(df: DataFrame): DataFrame = {
    df
      .filter(col("price") > 0)
      .filter(col("minimum_nights") <= 365)
  }

}
