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


    // Specify dataset and model names
    val datasetName = "dataset_32k.csv"
    val numTree = 20
    val maxDepth =15
    val featureStra ="auto"
    val modelName = "RFRegression-"+datasetName+"-numTree"+numTree+"-maxDepth"+maxDepth+"-featureStra_"+featureStra


    var dataset = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(0)+"/"+datasetName)
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

//    println("Start training models.....")
//    val rf = new RandomForestRegressor()
//      .setLabelCol("label")
//      .setFeaturesCol("features")
//      .setMaxDepth(maxDepth)
//      .setFeatureSubsetStrategy(featureStra)
//      .setNumTrees(numTree)
//    val model = rf.fit(train)
//    model.save(args(1)+"/"+modelName)


    println("Loading pretrianed model")
    val model = RandomForestRegressionModel.load(args(1)+"/"+modelName)
    // Make predictions.
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Dataset size "+ dataset.count())
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)


  }

}
