import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.evaluation.RegressionEvaluator
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


    val Array(train,test) = dataset.randomSplit(Array(0.8,0.2))

    // Train a RandomForest model.
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxDepth(14)
      .setFeatureSubsetStrategy("auto")
      .setNumTrees(200)


    //val model = rf.fit(train)
    //model.save("model_saved/RandomForestRegression")


    val model = RandomForestRegressionModel.load("model_saved/RandomForestRegression")
    // Make predictions.
    val predictions = model.transform(test)

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("dataset size "+ dataset.count())
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)



  }




}
