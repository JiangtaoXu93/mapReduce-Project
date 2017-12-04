
import org.apache.spark.ml.feature.VectorAssembler

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}



import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}


object RandomForestRegression {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("SPARK ML")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    var dataset = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("dataset.csv")
      .distinct()
      .toDF("songId","taste_count","jam_count", "trackId", "price","download","confidence","famil","artHot","dur","loud","songHot","tempo")
      .cache()

    val assembler = new VectorAssembler()
        .setInputCols(Array("taste_count","jam_count", "price","famil","artHot","dur","loud","songHot","tempo"))
        .setOutputCol("features")
    dataset = assembler.transform(dataset).cache()
    dataset = dataset.select("download","features").cache()
    dataset = dataset.withColumnRenamed("download","label").cache()


    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(dataset)



    val Array(train,test) = dataset.randomSplit(Array(0.8,0.2))

    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")
      .setMaxDepth(10)
      .setFeatureSubsetStrategy("auto")
      .setNumTrees(50)


    // Chain indexer and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(featureIndexer, rf))

    // Train model. This also runs the indexer.
    val model = pipeline.fit(train)

    // Make predictions.
    val predictions = model.transform(test)

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)



  }




}
