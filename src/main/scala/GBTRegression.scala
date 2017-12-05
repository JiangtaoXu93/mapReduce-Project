import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{GBTRegressionModel,GBTRegressor}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}


object GBTRegression{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("GBTRegression")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // Specify dataset and model names
    val datasetName = "dataset_400k.csv"
    val maxIter = 20
    val maxDepth =15
    val modelName = "GBTRegression-"+datasetName+"-maxIter"+maxIter+"-maxDepth"+maxDepth

//    var dataset = sqlContext.read
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .option("inferSchema", "true")
//      .load(args(0)+"/"+datasetName)
//      .distinct()
//      .toDF("songId","taste_count","jam_count", "trackId", "price","download","confidence","famil","artHot","dur","loud","songHot","tempo")
//      .cache()
//
//    val assembler = new VectorAssembler()
//        .setInputCols(Array("taste_count","jam_count", "price","famil","artHot","dur","loud","songHot","tempo"))
//        .setOutputCol("features")
//    dataset = assembler.transform(dataset).cache()
//    dataset = dataset.select("download","features").cache()
//    dataset = dataset.withColumnRenamed("download","label").cache()


    var dataset = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(args(0)+"/"+datasetName)
      .distinct()
      .toDF("artFam","artHot","duration","loudness","songHot","tempo","meanPrice","download","confidence","Jamcount","tastecount")
      .na.drop()
      .cache()
    val assembler = new VectorAssembler()
      .setInputCols(Array("artFam","artHot","duration","loudness","songHot","tempo","meanPrice","download","Jamcount","tastecount"))
      .setOutputCol("features")
    dataset = assembler.transform(dataset).cache()
    dataset = dataset.select("download","features").cache()
    dataset = dataset.withColumnRenamed("download","label").cache()


    val Array(train,test) = dataset.randomSplit(Array(0.8,0.2))



//    println("Start training models.....")
//    val gbt = new GBTRegressor()
//      .setLabelCol("label")
//      .setFeaturesCol("features")
//      .setMaxIter(maxIter)
//      .setMaxDepth(maxDepth)
//    val model = gbt.fit(train)
//    model.save(args(1)+"/"+modelName)


    println("Loading pre-trained model")
    val model =  GBTRegressionModel.load(args(1)+"/"+modelName)

    println("Making predictions")
    val predictions = model.transform(test)

    // Select (prediction, true label) and compute test error.
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")
    val rmse = evaluator.evaluate(predictions)
    println("Dataset size "+dataset.count())
    println("Root Mean Squared Error (RMSE) on test data = " + rmse)

  }


}
